
import os

import pymysql

from src.utils import sqs
from src.utils.data_utils import get_main_db_connection
from src.utils.archive import request_archive, request_execution_payload, request_execution_payload_sepolia
from src.utils import subgraph
from src.utils.constants import SLOTS_PER_EPOCH

connection = get_main_db_connection()
connection.autocommit(True)

FINALITY_CHECKPOINTS_PATH = "/eth/v1/beacon/states/finalized/finality_checkpoints"
SHANGHAI_EPOCH = int(os.environ['SHANGHAI_EPOCH'])
MIN_SEPOLIA_EPOCH = int(os.environ['MIN_SEPOLIA_EPOCH'])

MAX_EPOCH_VALIDATOR = int(os.environ.get('MAX_WITHDRAWAL_EPOCH_VALIDATOR', 5))
VALIDATOR_WITHDRAWAL = 'INSERT INTO Validator_Withdrawals (bls_key, epoch, values_withdrawals, withdrawal_recipient) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE values_withdrawals= VALUES(values_withdrawals);'
VALIDATOR_WITHDRAWAL_RUNNER = 'INSERT IGNORE INTO Validator_Withdrawal_Runner (epoch, validators) VALUES (%s, %s)'
VALIDATOR_SLOT_WITHDRAWALS = 'INSERT INTO Validator_Slot_Withdrawals (validator, slot, amount, withdrawal_index) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE amount = VALUES (amount), withdrawal_index = VALUES(withdrawal_index)'

# Note: In order to avoid hitting lambda timeout limit
# Note: Subsequent epochs will be synced once lambda is invoked the following time
# Note: Sync process will continue until the head state is reached

def max_validator_epoch():
    q= 'SELECT MAX(epoch) FROM Validator_Withdrawal_Runner'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    return cursor.fetchone()['MAX(epoch)']

def validator_bls_key_indexes():
    q= 'SELECT bls_key, indexes FROM Validator_BLS_Key_Index'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)
    validators = cursor.fetchall()

    return [int(validator['indexes']) for validator in validators], {int(validator['indexes']): validator['bls_key'] for validator in validators}

def finalized_epochs():
    url= '/eth/v1/beacon/states/finalized/finality_checkpoints'
    try:
        finalized_state_response = request_archive(url)
        data = finalized_state_response.get('data').get('finalized').get('epoch')
    except Exception as E:
        raise Exception(E)
    
    return data

def shanghai_merged(slot):
    if int(slot) // 32> SHANGHAI_EPOCH:
        return True
    else:
        return False
    
def withdrawals_slot(slot):
    if shanghai_merged(slot):
        execution_payload= request_execution_payload(slot)
    else:
        execution_payload= request_execution_payload_sepolia(slot)
    if not execution_payload:
        return {},{}, {}
    else:
        validator= {}
        withdrawal_recipient = {}
        validator_withdrawal_index = {}
        for withdrawal in execution_payload:
            validator[withdrawal["validator_index"]] = validator.get(withdrawal["validator_index"], 0) + int(withdrawal["amount"])
            withdrawal_recipient[withdrawal["validator_index"]]= withdrawal["address"]
            validator_withdrawal_index[withdrawal["validator_index"]] = withdrawal['index']
        return validator, withdrawal_recipient, validator_withdrawal_index
    
def data_handler(event, context):

    validators, validator_bls_key_index= validator_bls_key_indexes()
    max_epoch = max_validator_epoch()
    if not max_epoch:
        max_epoch = int(MIN_SEPOLIA_EPOCH)
    finalized_epoch = int(finalized_epochs())
    max_epoch = int(max_epoch)
    print(f"Last Withdrawal Epoch {str(max_epoch)}")
    print(f"Finalized Epoch {str(finalized_epoch)}")
    max_withdrawal_epoch= max_epoch
    inputs = []
    inputs_slot = []
    for epoch in range(max_epoch + 1 , min(finalized_epoch + 1, max_epoch + MAX_EPOCH_VALIDATOR)):
        epoch_withdrawals= {}
        epoch_withdrawals_recipient= {}
        for slot in range(epoch * SLOTS_PER_EPOCH, (epoch + 1) * SLOTS_PER_EPOCH):
            
            withdrawals, withdrawal_recipient, validator_withdrawal_index= withdrawals_slot(slot)
            withdrawals_validator = {key: withdrawals[key] for key in withdrawals.keys() if int(key) in validators}

            for validator, withdrawal in withdrawals_validator.items():
                inputs_slot = inputs_slot + [(int(validator), int(slot), int(withdrawal), int(validator_withdrawal_index[validator]))]
                epoch_withdrawals[validator] = epoch_withdrawals.get(validator, 0) + withdrawal
                epoch_withdrawals_recipient[validator]= withdrawal_recipient[validator]
        
        inputs = inputs + [(validator_bls_key_index[int(validator)], epoch, epoch_withdrawals[validator], epoch_withdrawals_recipient[validator]) for validator in epoch_withdrawals.keys()]
        max_withdrawal_epoch = epoch

    cursor = connection.cursor()
    cursor.executemany(VALIDATOR_WITHDRAWAL, inputs)
    if max_withdrawal_epoch > max_epoch:
        cursor.execute(VALIDATOR_WITHDRAWAL_RUNNER, (max_withdrawal_epoch, len(inputs)))
    cursor.executemany(VALIDATOR_SLOT_WITHDRAWALS, inputs_slot)
    connection.commit()