import json
import os

import pymysql

from src.utils import sqs
from src.utils.data_utils import get_main_db_connection
from src.utils import subgraph

from src.utils.constants import ONE_GWEI

FINALITY_CHECKPOINTS_PATH = "/eth/v1/beacon/states/finalized/finality_checkpoints"
INDEX_VALIDATOR_START_QUEUE_URL= os.environ['INDEX_VALIDATOR_START_QUEUE_URL']

connection = get_main_db_connection()

def commit(inputs):
    q= f'INSERT INTO Validator_Start_Index (bls_key, lsd_validator, indexes) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE indexes= VALUES(indexes)'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.executemany(q, inputs)
    connection.commit()

def validator_start_index(bls_keys):
    if len(bls_keys)==0:
        return {}
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    if len(bls_keys)==1:
        q= f'SELECT bls_key, indexes, MIN(epoch) FROM Validator_Indexes WHERE bls_key = \"{bls_keys[0]}\" GROUP BY bls_key'
    else:
        q= f'SELECT bls_key, indexes, MIN(epoch) FROM Validator_Indexes WHERE bls_key in {bls_keys} GROUP BY bls_key'
    
    cursor.execute(q)

    return {i['bls_key']: int(i['indexes']) for i in cursor.fetchall()}

def bls_key_start_indexes():
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    q= f'SELECT DISTINCT bls_key FROM Validator_Start_Index'
    cursor.execute(q)

    return [validator['bls_key'] for validator in cursor.fetchall()]

def data_handler(event, context):
    bodies = [json.loads(record['body']) for record in event['Records']]
    print("Recieved= ", len(bodies))

    bls_keys = [body['bls_key'] for body in bodies]
    inputs= []
    validator_lsd_indexes = subgraph.validator_lsd(bls_keys)
    validator_stakehouse_indexes= validator_start_index(tuple(set(bls_keys)- set(validator_lsd_indexes.keys())))

    inputs = [(validator, False, validator_stakehouse_indexes.get(validator)) for validator in validator_stakehouse_indexes.keys()]
    inputs+= [(validator, True, validator_lsd_indexes.get(validator)) for validator in validator_lsd_indexes.keys()]
    
    commit(inputs)

    sqs.delete_sqs_messages(INDEX_VALIDATOR_START_QUEUE_URL, event)


def queue_handler(event, context):
    bls_keys= subgraph.fetch_all_bls_keys_from_subgraph()
    
    bls_keys_start_index = bls_key_start_indexes()
    messages= []

    for bls_key in list(set(bls_keys)- set(bls_keys_start_index)):

        bls_key_hash = sqs.hash_sqs(bls_key)
        messages.append(
            {
                'Id': f'{bls_key_hash}-{len(messages)}',
                'MessageBody': json.dumps({'bls_key': bls_key})
            }
        )

        if len(messages)== 10:
            sqs.post_sqs_messages(INDEX_VALIDATOR_START_QUEUE_URL, messages)
            messages= []

    if len(messages)> 0:
        sqs.post_sqs_messages(INDEX_VALIDATOR_START_QUEUE_URL, messages)