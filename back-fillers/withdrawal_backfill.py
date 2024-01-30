import os

import pymysql

import datetime

from utils.data_utils import get_main_db_connection
from utils.archive import request_archive, request_execution_payload, request_execution_payload_sepolia

connection = get_main_db_connection()
connection.autocommit(True)

FINALITY_CHECKPOINTS_PATH = "/eth/v1/beacon/states/finalized/finality_checkpoints"

MAX_EPOCH_VALIDATOR = int(os.environ.get('MAX_WITHDRAWAL_EPOCH_VALIDATOR', 5))
VALIDATOR_WITHDRAWAL = 'INSERT INTO Validator_Withdrawals (bls_key, epoch, values_withdrawals, withdrawal_recipient) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE values_withdrawals= VALUES(values_withdrawals);'
VALIDATOR_WITHDRAWAL_RUNNER = 'INSERT IGNORE INTO Validator_Withdrawal_Runner (epoch, validators) VALUES (%s, %s)'
VALIDATOR_SLOT_WITHDRAWALS = 'INSERT INTO Validator_Slot_Withdrawals (validator, slot, amount, withdrawal_index) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE amount = VALUES (amount), withdrawal_index = VALUES(withdrawal_index)'
VALIDATOR_SLOT_WITHDRAWALS_BACKFILLER = 'INSERT INTO validator_withdrawal_backfiller (run, slot_lower, slot_upper, withdrawals) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE slot_lower = VALUES (slot_lower), slot_upper = VALUES(slot_upper), withdrawals = VALUES(withdrawals)'

# Note: In order to avoid hitting lambda timeout limit
# Note: Subsequent epochs will be synced once lambda is invoked the following time
# Note: Sync process will continue until the head state is reached

SHANGHAI_SLOT = (162309 * 32)
SLOTS_PROCESSOR = 100


def max_validator_epoch():
    q = 'SELECT MAX(epoch) FROM Validator_Withdrawal_Runner'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    return cursor.fetchone()['MAX(epoch)']


def get_slots(skip: int, slots: int):
    return list(range(SHANGHAI_SLOT + skip, SHANGHAI_SLOT + skip + slots))


def validator_bls_key_indexes():
    q = 'SELECT bls_key, indexes FROM Validator_BLS_Key_Index'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)
    validators = cursor.fetchall()

    return [int(validator['indexes']) for validator in validators], {int(validator['indexes']): validator['bls_key'] for validator in validators}


def finalized_epochs():
    url = '/eth/v1/beacon/states/finalized/finality_checkpoints'
    try:
        finalized_state_response = request_archive(url)
        data = finalized_state_response.get(
            'data').get('finalized').get('epoch')
    except Exception as E:
        raise Exception(E)

    return data


def withdrawals_slot(slot):
    start_request = datetime.datetime.now()
    execution_payload, status_validator = request_execution_payload(slot)
    total_time = datetime.datetime.now() - start_request
    if not execution_payload:
        return {}, {}, {}, status_validator, total_time
    else:
        validator = {}
        withdrawal_recipient = {}
        validator_withdrawal_index = {}
        for withdrawal in execution_payload:
            validator[withdrawal["validator_index"]] = validator.get(
                withdrawal["validator_index"], 0) + int(withdrawal["amount"])
            withdrawal_recipient[withdrawal["validator_index"]
                                 ] = withdrawal["address"]
            validator_withdrawal_index[withdrawal["validator_index"]
                                       ] = withdrawal['index']
        return validator, withdrawal_recipient, validator_withdrawal_index, status_validator, total_time


def data_handler(skip: int):
    print(f"Run {int(skip / SLOTS_PROCESSOR)}")
    validators, validator_bls_key_index = validator_bls_key_indexes()
    slots = get_slots(skip, SLOTS_PROCESSOR)
    inputs_slot = []
    for slot in slots:

        withdrawals, withdrawal_recipient, validator_withdrawal_index, status_validator, total_time = withdrawals_slot(
            slot)
        print(
            f"Processing Slot {slot} in Run {int(skip / SLOTS_PROCESSOR)}, {status_validator} time taken {total_time}")
        withdrawals_validator = {
            key: withdrawals[key] for key in withdrawals.keys() if int(key) in validators}

        for validator, withdrawal in withdrawals_validator.items():
            inputs_slot = inputs_slot + \
                [(int(validator), int(slot), int(withdrawal),
                  int(validator_withdrawal_index[validator]))]

    cursor = connection.cursor()
    cursor.executemany(VALIDATOR_SLOT_WITHDRAWALS, inputs_slot)
    cursor.execute(VALIDATOR_SLOT_WITHDRAWALS_BACKFILLER, (int(
        skip / SLOTS_PROCESSOR), slots[0], slots[-1], len(inputs_slot)))

    connection.commit()
    print(f"Run {skip / SLOTS_PROCESSOR} Processed {len(inputs_slot)} withdrawals")
    print(
        f"Finished {((slots[-1] - SHANGHAI_SLOT)/ ((200240*32) - SHANGHAI_SLOT))* 100} % slots\n\n")
    return False


for slot_skip in range(7836453, 8227668):
    s = data_handler(slot_skip * SLOTS_PROCESSOR)