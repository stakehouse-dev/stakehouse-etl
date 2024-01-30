import json
import os
import schedule
import time

import pymysql

from utils.data_utils import get_main_db_connection, VALIDATOR_INSERT_QUERY
from utils.archive import request_archive, get_validator_url
from utils import subgraph

connection = get_main_db_connection()

FINALITY_CHECKPOINTS_PATH = "/eth/v1/beacon/states/finalized/finality_checkpoints"

RATE_LIMIT_THRESHOLD = os.environ.get('RATE_LIMIT_THRESHOLD', 150)
SLEEP_LENGTH = os.environ.get('SLEEP_LENGTH', 10)
RATE_LIMIT_UPDATE= int(os.environ.get('RATE_LIMIT_UPDATE', 200))

# Note: In order to avoid hitting lambda timeout limit
# Note: Subsequent epochs will be synced once lambda is invoked the following time
# Note: Sync process will continue until the head state is reached
EPOCHS_PER_VALIDATOR_LIMIT = 10


def activation_epochs(bls_keys):
    validator_epoch= request_archive(get_validator_url(), params={"id": bls_keys}).get('data', None)

    if not validator_epoch:
        raise Exception("Activation Epoch")
    activationEpochs= {validator.get('validator').get('pubkey'): validator.get('validator').get('activation_epoch')
                        for validator in validator_epoch}
    
    return activationEpochs

def all_epoch_balances(bls_keys, epoch):
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    if len(bls_keys)== 0:
        return []
    if len(bls_keys)>1:
        INDEX_MAX_EPOCH= f'SELECT bls_key, epoch FROM Validator_Balances WHERE bls_key in {tuple(bls_keys)} and epoch > {epoch};'
    else:
        INDEX_MAX_EPOCH= f'SELECT bls_key, epoch FROM Validator_Balances WHERE bls_key= \"{bls_keys[0]}\" and epoch> {epoch};'
    cursor.execute(INDEX_MAX_EPOCH)

    
    validators = (cursor.fetchall())
    validator_epochs= {bls_key: [int(i['epoch']) for i in validators if i['bls_key'] == bls_key] for bls_key in bls_keys}

    return validator_epochs

def index_validators():
    q= f'SELECT bls_key FROM Validator_Start_Index WHERE indexes != 0'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    validator_indexes = []

    for validator in cursor.fetchall():

        validator_indexes.append(validator['bls_key'])
    
    return validator_indexes


# Note: Function gets the oldest EPOCHS_PER_VALIDATOR_LIMIT needed to be synced
def get_unsynced_epochs(bls_key, current_epoch):
    q = f'SELECT MAX(epoch) FROM Validator_Balances WHERE bls_key = \"{bls_key}\";'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)
    result = cursor.fetchone()

    reference_epoch = result.get('MAX(epoch)')

    if not reference_epoch:
        resp = request_archive(get_validator_url(), {'id': [bls_key]})
        consensus_data = resp.get('data')

        if not consensus_data:
            raise Exception('Failed to fetch data from archive')

        if len(consensus_data) != 1:
            raise Exception('Invalid consensus query result')

        activation_epoch = consensus_data[0].get('validator', {}).get('activation_epoch')

        if not activation_epoch:
            raise Exception('Invalid consensus query result')

        reference_epoch = activation_epoch

    reference_epoch = int(reference_epoch)
    catchup = []

    for epoch in range(reference_epoch + 1, current_epoch + 1):
        catchup.append(epoch)

        # Limit is introduced to avoid memory and runtime limitations
        # Long epoch sync is chunked into smaller pieces instead
        if len(catchup) == EPOCHS_PER_VALIDATOR_LIMIT:
            return catchup

    return catchup


def data_handler(event):

    record_index = {}

    for record in event:
        parsed_record = json.loads(record['body'])
        epoch_number = parsed_record['epoch_number']
        bls_key = parsed_record['bls_key']

        record_index[epoch_number] = record_index.get(epoch_number, []) + [bls_key]

    inputs = []
    for epoch_number in record_index:
        bls_keys = record_index[epoch_number]
        url = f'/eth/v1/beacon/states/{epoch_number * 32}/validators'

        finalized_state_response = request_archive(url, {'id': bls_keys})
        data = finalized_state_response.get('data')

        if not data:
            raise Exception('Failed to fetch archive node data')

        for vd in data:
            inputs.append((vd['validator']['pubkey'], epoch_number, vd['balance']))

    
    cursor = connection.cursor()
    cursor.executemany(VALIDATOR_INSERT_QUERY, inputs)
    connection.commit()

def validator_backfiller_queue_handler():

    print("Backfiller Running")
    try:
        bls_keys= index_validators()[:20]
        bls_key_groups= [bls_keys[i:i+ 10] for i in range(0, len(bls_keys), 10)]

        f = open("epoch.txt", "r")
        last_epoch = int(f.read())
        f.close()

        MAX_LAST_EPOCH = []

        bls_key_len = len(bls_keys)
        i= 0
        for bls_key_group in bls_key_groups:
            activation_epoch= activation_epochs(bls_key_group)
            epochs= all_epoch_balances(bls_key_group, last_epoch)
            for bls_key in [bls_key for bls_key in list(epochs.keys()) if len(epochs[bls_key])!= 0]:
                i= i+1
                print(f"Validator {i} of")
                print(bls_key_len)
                MAX_LAST_EPOCH.append(max(epochs[bls_key]))
                epochs_to_update= list(set(range(max(last_epoch, int(activation_epoch[bls_key]))+ 1, max(epochs[bls_key]) + 1))- set(epochs[bls_key]))
                print(bls_key)

                messages= []
                total_epochs = len(epochs_to_update)
                epoch_processes = 0
                for epoch in epochs_to_update:
                    print(f"Processes Epoch {epoch_processes} out of {total_epochs} epochs")
                    epoch_processes= epoch_processes + 1
                    if epoch_processes % 10 == 0:
                        print(f"Processed Epoch {epoch}")
                    messages.append(
                        {
                            'body': json.dumps({'bls_key': bls_key, 'epoch_number': epoch})
                        }
                    )

                    if len(messages)== 10:
                        data_handler(messages)
                        messages= []

                if len(messages)> 0:
                    data_handler(messages)
    except:
        pass


validator_backfiller_queue_handler()
