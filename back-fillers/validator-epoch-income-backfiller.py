import json
import os
import schedule
import time
from datetime import datetime

import pymysql

from utils.data_utils import get_main_db_connection, APR_EPOCH_INSERT_QUERY, VALIDATOR_EPOCH_RUNNER_INSERT_QUERY
from utils.archive import request_archive, get_validator_url
from utils import subgraph
from utils.financials import calc_apr

connection = get_main_db_connection()
connection.autocommit(True)

RATE_LIMIT_THRESHOLD = os.environ.get('RATE_LIMIT_THRESHOLD', 150)
SLEEP_LENGTH = os.environ.get('SLEEP_LENGTH', 10)
RATE_LIMIT_UPDATE= int(os.environ.get('RATE_LIMIT_UPDATE', 200))

# Note: In order to avoid hitting lambda timeout limit
# Note: Subsequent epochs will be synced once lambda is invoked the following time
# Note: Sync process will continue until the head state is reached
EPOCHS_PER_VALIDATOR_LIMIT = 10

# Function to get the last updated epoch for which the apr exist for the bls keys.
# The function returns a dictionary where each bls key refers to the maximum epoch for
# which the indexes have been updated as well as the cumulative earning upto the last
# updated epoch and epochs since active epoch for the validator.
def get_epochs(bls_keys):
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    if len(bls_keys) == 1:
        INDEX_MAX_EPOCH= f'SELECT a.bls_key, a.epoch, a.earnings, a.losses, a.epochs_since_active FROM Validator_Epoch_Income a INNER JOIN ( SELECT bls_key, MAX(epoch) epoch FROM Validator_Epoch_Income WHERE bls_key = \"{bls_keys[0]}\" GROUP BY bls_key ) b ON a.bls_key= b.bls_key AND a.epoch = b.epoch'
    else:
        INDEX_MAX_EPOCH= f'SELECT a.bls_key, a.epoch, a.earnings, a.losses, a.epochs_since_active FROM Validator_Epoch_Income a INNER JOIN ( SELECT bls_key, MAX(epoch) epoch FROM Validator_Epoch_Income WHERE bls_key in {bls_keys} GROUP BY bls_key ) b ON a.bls_key= b.bls_key AND a.epoch = b.epoch'
    cursor.execute(INDEX_MAX_EPOCH)
    validators = (cursor.fetchall())
    validator_epochs= {validator['bls_key']: {'MAX(epoch)': int(validator['epoch']), 'earnings': float(validator['earnings']), 'losses': float(validator['losses']), 'epochs_since_active': float(validator['epochs_since_active'])} for validator in validators}
    
    return validator_epochs

# Gives Max Epoch For Which Withdrawals Have Been Processed
def validator_withdrawal_epoch():
    q= 'SELECT MAX(epoch) FROM Validator_Withdrawal_Runner'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    return cursor.fetchone()['MAX(epoch)']

# This function returns a list of epoch wise balance between specified epochs
def validator_balances(bls_key, lower, upper):
    bls_key= json.dumps(bls_key)
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    INDEX_MAX_EPOCH= f'SELECT epoch, balance FROM Validator_Balances WHERE bls_key= {bls_key} AND epoch BETWEEN {lower} AND {upper}'
    cursor.execute(INDEX_MAX_EPOCH)
    validators = (cursor.fetchall())
    
    return validators

# This function gets the maximum epoch for which balance exist in Validator_Balances Table.
# For each validator the function returns max epoch.
def epoch_upper_balance(bls_keys):
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    if len(bls_keys) == 1:
        INDEX_MAX_EPOCH= f'SELECT bls_key, MAX(epoch) FROM Validator_Balances WHERE bls_key= \"{bls_keys[0]}\" GROUP BY bls_key;'
    else:
        INDEX_MAX_EPOCH= f'SELECT bls_key, MAX(epoch) FROM Validator_Balances WHERE bls_key in {bls_keys} GROUP BY bls_key;'
    cursor.execute(INDEX_MAX_EPOCH)
    validators = (cursor.fetchall())
    validator_epochs= {validator['bls_key']: int(validator['MAX(epoch)']) for validator in validators}
    
    return validator_epochs

# This function gives the list of epoch wise apr for all the bls keys with max epoch
def get_validator_income_max_epoch(bls_keys, lastUpdateEpochs, max_withdrawal_epoch):
    
    validator_apr= []
    validator_epoch_runner = []
    epoch_upper= epoch_upper_balance(tuple(bls_keys))    
    for bls_key in bls_keys:
        
        lastUpdateEpoch= int(lastUpdateEpochs[bls_key]['MAX(epoch)'])
        epoch_to_update= min(epoch_upper[bls_key], max_withdrawal_epoch)
        balances= validator_balances(bls_key, lastUpdateEpoch, epoch_to_update)
        withdrawals= validator_withdrawals(bls_key, lastUpdateEpoch, epoch_to_update)
        epoch_apr= validator_epoch_apr(bls_key, balances, withdrawals, lastUpdateEpochs[bls_key]['earnings'], 
                                       lastUpdateEpochs[bls_key]['losses'], lastUpdateEpochs[bls_key]['epochs_since_active'])

        if len(epoch_apr)== 0:
            continue

        validator_apr= validator_apr + epoch_apr
        validator_epoch_runner = validator_epoch_runner + [(epoch_apr[-1][0], epoch_apr[-1][1])]

    return validator_apr, validator_epoch_runner

# This function gives the list of epoch wise apr for all the bls keys without max epoch
def get_validator_income_without_max_epoch(bls_keys, activation_epochs, max_withdrawal_epoch):

    validator_epoch_runner = []
    validator_apr= []
    epoch_upper= epoch_upper_balance(tuple(bls_keys))
    for bls_key in bls_keys:

        epoch_to_update= min(epoch_upper[bls_key], max_withdrawal_epoch)
        balances= validator_balances(bls_key, int(activation_epochs[bls_key]) + 1, epoch_to_update)
        withdrawals= validator_withdrawals(bls_key, activation_epochs[bls_key], epoch_to_update)
        epoch_apr= validator_epoch_apr_without_max_epoch(bls_key, balances, withdrawals, activation_epochs[bls_key])

        if len(epoch_apr)== 0:
            continue

        validator_apr= validator_apr + epoch_apr
        validator_epoch_runner = validator_epoch_runner + [(epoch_apr[-1][0], epoch_apr[-1][1])]

    return validator_apr, validator_epoch_runner

# Function to enter apr into SQL for the epochs
def commit(inputs, input_bls_key_epoch_runner):
    if len(inputs) == 0:
        return
    cursor = connection.cursor()
    cursor.executemany(APR_EPOCH_INSERT_QUERY, inputs)
    cursor.executemany(VALIDATOR_EPOCH_RUNNER_INSERT_QUERY, input_bls_key_epoch_runner)
    connection.commit()

# Withdrawals Of The Validator
def validator_withdrawals(bls_key, lastUpdateEpoch, epoch_to_update):
    q= f'SELECT epoch, values_withdrawals FROM Validator_Withdrawals WHERE bls_key= \"{str(bls_key)}\" AND epoch BETWEEN {lastUpdateEpoch} AND {epoch_to_update}'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    return {int(withdrawal['epoch']): float(withdrawal['values_withdrawals']) for withdrawal in cursor.fetchall()}

# This function is for calculating apr for bls key for which epoch wise apr exists.
# This function takes cumulative earning till the last updated epoch and epochs since
# activation epoch. For each epoch the function calculates the earnings and apr.
def validator_epoch_apr(bls_key, balances, withdrawals, earning, losses, epochs):
    cumulative_earnings= earning
    cumulative_losses= losses
    epochs_since_active= epochs
    validator_apr= []
    print(f'\nbls key {bls_key}')
    for i in range(len(balances)-1):
        print(f'\nprocessing {i}')
        print(f'current epoch', balances[i]['epoch'])
        print(f'next epoch', balances[i+1]['epoch'])
        if int(balances[i+1]['epoch']) - int(balances[i]['epoch'])> 1:
            # If gap in earnings is to large, balance difference can just be filled in
            # if int(balances[i]['epoch']) == int('242501'):
            #     print('\n****filling in earnings****')
            #     earningPerEpoch = float('2870.0')
            #     balanceNow = float(balances[i]['balance'])
            #     for j in range(2389):
            #         epochToProcess = int(balances[i]['epoch'])+j
            #         print(f'\nprocessing epoch', epochToProcess)
            #         nextBalance = balanceNow + earningPerEpoch
            #         print(f'balance {balanceNow}')
            #         print(f'next balance {nextBalance}')
            #         print(f'withdrawals', withdrawals.get(balances[i]['epoch'], 0))
            #         cumulative_earnings+= max(
            #                         0, 
            #                             (
            #                             nextBalance
            #                             + withdrawals.get(balances[i]['epoch'], 0)
            #                             - balanceNow
            #                             )
            #                      )
            #         cumulative_losses-= min(
            #                                     0,
            #                                         (
            #                                         nextBalance
            #                                         + withdrawals.get(balances[i]['epoch'], 0)
            #                                         - balanceNow
            #                                         )
            #                                 )
            #         epochs_since_active += 1
            #         apr= calc_apr(cumulative_earnings, epochs_since_active)
            #         validator_apr.append((bls_key, epochToProcess+1, cumulative_earnings, cumulative_losses, apr, epochs_since_active))
            #         balanceNow += earningPerEpoch
            #     continue
            #     print('\n****end of filling in earnings****')
            print("Backfiller Needed")
            break
        cumulative_earnings+= max(
                                    0, 
                                        (
                                        float(balances[i+1]['balance'])
                                        + withdrawals.get(balances[i]['epoch'], 0)
                                        - float(balances[i]['balance'])
                                        )
                                 )
        cumulative_losses-= min(
                                    0,
                                        (
                                        float(balances[i+1]['balance'])
                                        + withdrawals.get(balances[i]['epoch'], 0)
                                        - float(balances[i]['balance'])
                                        )
                                )
        epochs_since_active += 1
        apr= calc_apr(cumulative_earnings, epochs_since_active)
        validator_apr.append((bls_key, balances[i+1]['epoch'], cumulative_earnings, cumulative_losses, apr, epochs_since_active))
    
    return validator_apr

# Function to get activation epochs for the specified bls keys
def activation_epochs(bls_keys):
    validator_epoch= request_archive(get_validator_url(), params={"id": bls_keys}).get('data', None)
    if not validator_epoch:
        raise Exception("No Activation Epoch")
    activationEpochs= {validator.get('validator').get('pubkey'): validator.get('validator').get('activation_epoch')
                        for validator in validator_epoch}
    
    return activationEpochs

def data_handler(bls_keys):

    try:
        maxEPOCHS= get_epochs(tuple(bls_keys))
        bls_keys_with_max_epoch= list(maxEPOCHS.keys())
        bls_keys_without_max_epoch= list(set(bls_keys)- set(bls_keys_with_max_epoch))
    except Exception as e:
        logger.error(e)
        raise Exception
    
    max_withdrawal_epoch = int(validator_withdrawal_epoch())

    if len(bls_keys_with_max_epoch)>0:
        input_bls_key_with_max_epoch, input_bls_key_with_max_epoch_runner= get_validator_income_max_epoch(bls_keys_with_max_epoch, maxEPOCHS, max_withdrawal_epoch)
        commit(input_bls_key_with_max_epoch, input_bls_key_with_max_epoch_runner)

    if len(bls_keys_without_max_epoch)>0:
        activation_epoch= activation_epochs(bls_keys_without_max_epoch)
        input_bls_key_without_max_epoch, input_bls_key_without_max_epoch_runner= get_validator_income_without_max_epoch(bls_keys_without_max_epoch, activation_epoch, max_withdrawal_epoch)
        commit(input_bls_key_without_max_epoch, input_bls_key_without_max_epoch_runner)

def validator_backfiller_queue_handler():

    print("Backfiller Running Mainnet")
    print(datetime.now().strftime('%d-%m-%Y %H:%M:%S'))
    try:
        data_handler(["0x96ad445342272fd198fe1cb8193b40bf6fcfd3c74a6555b5c39a4c3f05a2c44c9f2327768716b8c77c29b376ed0fcf49"])
    except Exception as e:
        print("Failed Backfiller")
        print(e)
        print("\n")

validator_backfiller_queue_handler()