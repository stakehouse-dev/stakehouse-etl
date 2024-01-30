import json
import os

import pymysql

from src.utils import sqs
from src.utils.data_utils import get_main_db_connection, APR_EPOCH_INSERT_QUERY, VALIDATOR_EPOCH_RUNNER_INSERT_QUERY
from src.utils.archive import request_archive, get_validator_url
from src.utils import subgraph
from src.utils.financials import calc_apr
from src.utils.logging import logs

FINALITY_CHECKPOINTS_PATH = "/eth/v1/beacon/states/finalized/finality_checkpoints"
VALIDATOR_EPOCH_INCOME_QUEUE_URL= os.environ['VALIDATOR_EPOCH_INCOME_QUEUE_URL']

connection = get_main_db_connection()
connection.autocommit(True)

# This is directed towards fetching the apr of validators epoch wise
# The lambda runs every epoch and calculates and assign cumulative earning as well
# as apr of the validators. This file contains the handler  function as well
# as the helper functions.


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

# Withdrawals Of The Validator
def validator_withdrawals(bls_key, lastUpdateEpoch, epoch_to_update):
    q= f'SELECT epoch, values_withdrawals FROM Validator_Withdrawals WHERE bls_key= \"{str(bls_key)}\" AND epoch BETWEEN {lastUpdateEpoch} AND {epoch_to_update}'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    return {int(withdrawal['epoch']): float(withdrawal['values_withdrawals']) for withdrawal in cursor.fetchall()}

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

# This function returns a list of epoch wise balance between specified epochs
def validator_balances(bls_key, lower, upper):
    bls_key= json.dumps(bls_key)
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    INDEX_MAX_EPOCH= f'SELECT epoch, balance FROM Validator_Balances WHERE bls_key= {bls_key} AND epoch BETWEEN {lower} AND {upper}'
    cursor.execute(INDEX_MAX_EPOCH)
    validators = (cursor.fetchall())
    
    return validators

# Function to get activation epochs for the specified bls keys
def activation_epochs(bls_keys):
    validator_epoch= request_archive(get_validator_url(), params={"id": bls_keys}).get('data', None)
    if not validator_epoch:
        raise Exception("No Activation Epoch")
    activationEpochs= {validator.get('validator').get('pubkey'): validator.get('validator').get('activation_epoch')
                        for validator in validator_epoch}
    
    return activationEpochs

# Function to enter apr into SQL for the epochs
def commit(inputs, input_bls_key_epoch_runner):
    if len(inputs) == 0:
        return
    cursor = connection.cursor()
    cursor.executemany(APR_EPOCH_INSERT_QUERY, inputs)
    cursor.executemany(VALIDATOR_EPOCH_RUNNER_INSERT_QUERY, input_bls_key_epoch_runner)
    connection.commit()

# This function is for calculating apr for bls key for which epoch wise apr exists.
# This function takes cumulative earning till the last updated epoch and epochs since
# activation epoch. For each epoch the function calculates the earnings and apr.
def validator_epoch_apr(bls_key, balances, withdrawals, earning, losses, epochs):
    cumulative_earnings= earning
    cumulative_losses= losses
    epochs_since_active= epochs
    validator_apr= []
    for i in range(len(balances)-1):
        if int(balances[i+1]['epoch']) - int(balances[i]['epoch'])> 1:
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

# This function is for calculating apr for bls key for which epoch wise apr doesn't exist.
# This function takes cumulative earning as balance of the first epoch - 32 and epochs since
# activation epoch. For each epoch the function calculates the earnings and apr.
def validator_epoch_apr_without_max_epoch(bls_key, balances, withdrawals, activation_epoch):
    cumulative_earnings = 0
    cumulative_losses= 0
    epochs_since_active= float(balances[0]['epoch'])- float(activation_epoch)
    validator_apr= []
    for i in range(len(balances)-1):
        if int(balances[i+1]['epoch']) - int(balances[i]['epoch'])> 1:
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
        epochs_since_active+= 1 
        apr= calc_apr(cumulative_earnings, epochs_since_active)
        validator_apr.append((bls_key, balances[i+1]['epoch'], cumulative_earnings, cumulative_losses, apr, epochs_since_active))
    
    return validator_apr

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

def data_handler(event, context):
    logger= logs()
    bodies = [json.loads(record['body']) for record in event['Records']]
    print("Recieved= ", len(bodies))

    try:
        bls_keys= [body['bls_key'] for body in bodies]
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
    
    logger.handlers.clear()
    sqs.delete_sqs_messages(VALIDATOR_EPOCH_INCOME_QUEUE_URL, event)


# Function that sends all the validators along with the epoch 10 at a time
def queue_handler(event, context):

    messages= []
    for bls_key in subgraph.fetch_all_bls_keys_from_subgraph():
        bls_key_hash = sqs.hash_sqs(bls_key)
        messages.append(
            {
                'Id': f'{bls_key_hash}-{len(messages)}',
                'MessageBody': json.dumps({'bls_key': bls_key})
            }
        )

        if len(messages)== 10:
            sqs.post_sqs_messages(VALIDATOR_EPOCH_INCOME_QUEUE_URL, messages)
            messages= []

    if len(messages)> 0:
        sqs.post_sqs_messages(VALIDATOR_EPOCH_INCOME_QUEUE_URL, messages)


