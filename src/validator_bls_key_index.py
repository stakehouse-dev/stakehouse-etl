import json
import os
from hashlib import sha256

import pymysql

from src.utils import sqs
from src.utils.data_utils import get_main_db_connection
from src.utils.archive import request_archive
from src.utils import subgraph

connection = get_main_db_connection()
connection.autocommit(True)

FINALITY_CHECKPOINTS_PATH = "/eth/v1/beacon/states/finalized/finality_checkpoints"
VALIDATOR_BLS_KEY_INDEXES_QUEUE_URL = os.environ['VALIDATOR_BLS_KEY_INDEXES_QUEUE_URL']

RATE_LIMIT_THRESHOLD = os.environ.get('RATE_LIMIT_THRESHOLD', 150)
SLEEP_LENGTH = os.environ.get('SLEEP_LENGTH', 10)
VALIDATOR_INSERT_QUERY = 'INSERT INTO Validator_BLS_Key_Index (bls_key, indexes) VALUES (%s, %s) ON DUPLICATE KEY UPDATE indexes= VALUES(indexes);'

# Note: In order to avoid hitting lambda timeout limit
# Note: Subsequent epochs will be synced once lambda is invoked the following time
# Note: Sync process will continue until the head state is reached
EPOCHS_PER_VALIDATOR_LIMIT = 10


def bls_key_index():
    q= 'SELECT DISTINCT bls_key FROM Validator_BLS_Key_Index'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    return [index['bls_key'] for index in cursor.fetchall()]


def data_handler(event, context):

    record_index = []
    for record in event['Records']:
        parsed_record = json.loads(record['body'])
        bls_key = parsed_record['bls_key']

        record_index = record_index + [bls_key]

    print('Received number of events:', len(event['Records']))

    inputs = []

    url = f'/eth/v1/beacon/states/finalized/validators'

    try:
        finalized_state_response = request_archive(url, {'id': record_index})
        data = finalized_state_response.get('data')
    except Exception as E:
        raise Exception(E)

    if not data:
        print('Finalized state response:', finalized_state_response)
        print('ERRORED BLS KEYS:', record_index)
        raise Exception('Failed to fetch archive node data')

    for vd in data:
        inputs.append((vd['validator']['pubkey'], vd['index']))

    
    cursor = connection.cursor()
    cursor.executemany(VALIDATOR_INSERT_QUERY, inputs)
    connection.commit()

    sqs.delete_sqs_messages(VALIDATOR_BLS_KEY_INDEXES_QUEUE_URL, event)


def queue_handler(event, context):

    bls_key_validators = subgraph.fetch_all_bls_keys_from_subgraph()

    bls_keys_with_index = bls_key_index()

    if len(list(set(bls_key_validators)- set(bls_keys_with_index))) == 0:
        print("No Validators")
    messages= []
    for bls_key in list(set(bls_key_validators)- set(bls_keys_with_index)):

        bls_key_hash = sha256(bls_key.encode('utf-8')).hexdigest()
        messages = messages + [
            {
                'Id': f'{bls_key_hash}',
                'MessageBody': json.dumps({'bls_key': bls_key})
            }
        ]

        if len(messages) > 20:
            sqs.post_sqs_messages(VALIDATOR_BLS_KEY_INDEXES_QUEUE_URL, messages)
            messages= []
    
    if len(messages)> 0:
        sqs.post_sqs_messages(VALIDATOR_BLS_KEY_INDEXES_QUEUE_URL, messages)