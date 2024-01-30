import json
import os
from hashlib import sha256

import pymysql

from src.utils import sqs
from src.utils.data_utils import get_main_db_connection, VALIDATOR_INSERT_QUERY
from src.utils.archive import request_archive, get_validator_url
from src.utils import subgraph

connection = get_main_db_connection()
connection.autocommit(True)

FINALITY_CHECKPOINTS_PATH = "/eth/v1/beacon/states/finalized/finality_checkpoints"
QUEUE_URL = os.environ['QUEUE_URL']

RATE_LIMIT_THRESHOLD = os.environ.get('RATE_LIMIT_THRESHOLD', 150)
SLEEP_LENGTH = os.environ.get('SLEEP_LENGTH', 10)

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

def all_epoch_balances(bls_keys):
    cursor = connection.cursor(pymysql.cursors.DictCursor)

    if len(bls_keys)== 0:
        return []
    if len(bls_keys)>1:
        INDEX_MAX_EPOCH= f'SELECT bls_key, epoch FROM Validator_Balances WHERE bls_key in {tuple(bls_keys)};'
    else:
        INDEX_MAX_EPOCH= f'SELECT bls_key, epoch FROM Validator_Balances WHERE bls_key= \"{bls_keys[0]}\";'
    cursor.execute(INDEX_MAX_EPOCH)
    validators = (cursor.fetchall())
    validator_epochs= {bls_key: [int(i['epoch']) for i in validators if i['bls_key'] == bls_key] for bls_key in bls_keys}

    return validator_epochs


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


def data_handler(event, context):

    record_index = {}

    for record in event['Records']:
        parsed_record = json.loads(record['body'])
        epoch_number = parsed_record['epoch_number']
        bls_key = parsed_record['bls_key']

        record_index[epoch_number] = record_index.get(epoch_number, []) + [bls_key]

    print('Received number of events:', len(event['Records']))

    inputs = []
    for epoch_number in record_index:
        bls_keys = record_index[epoch_number]
        url = f'/eth/v1/beacon/states/{(epoch_number * 32) - 1}/validators'

        try:
            finalized_state_response = request_archive(url, {'id': bls_keys})
            data = finalized_state_response.get('data')
        except Exception as E:
            raise Exception(E)
            
        print('Processing epoch number', epoch_number, 'For bls public keys', bls_keys)

        if not data:
            print('Finalized state response:', finalized_state_response)
            print('ERRORED BLS KEYS:', bls_keys)
            raise Exception('Failed to fetch archive node data')

        print('Data processed successfully:', data)

        for vd in data:
            inputs.append((vd['validator']['pubkey'], epoch_number, vd['balance']))

    
    cursor = connection.cursor()
    cursor.executemany(VALIDATOR_INSERT_QUERY, inputs)
    connection.commit()

    sqs.delete_sqs_messages(QUEUE_URL, event)


def queue_handler(event, context):
    # try:
    epoch_response = request_archive(FINALITY_CHECKPOINTS_PATH)

    if not epoch_response or not epoch_response.get('data'):
        raise Exception('Failed to fetch current epoch')

    current_epoch = epoch_response.get('data', {}).get('finalized', {}).get('epoch')

    if not current_epoch:
        raise Exception('Failed to fetch current epoch')

    current_epoch = int(current_epoch)

    print('Latest epoch:', current_epoch)

    # counter = 0
    for bls_key in subgraph.fetch_all_bls_keys_from_subgraph():
        # counter += 1

        bls_key_hash = sha256(bls_key.encode('utf-8')).hexdigest()
        messages = [
            {
                'Id': f'{bls_key_hash}-{current_epoch}',
                'MessageBody': json.dumps({'bls_key': bls_key, 'epoch_number': current_epoch})
            }
        ]

        # TODO: Possible improvement for @subhashish. Figure out which bls public keys are already in the database and which aren't and chunk activation epoch requests into bigger pieces

        sqs.post_sqs_messages(QUEUE_URL, messages)