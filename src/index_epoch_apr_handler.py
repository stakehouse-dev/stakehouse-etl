import json
import os

import pymysql

from src.utils import sqs
from src.utils.data_utils import get_main_db_connection, INDEX_APR_INSERT_QUERY
from src.utils.archive import request_archive
from src.utils import subgraph

connection = get_main_db_connection()
connection.autocommit(True)

API_KEY = os.environ['ARCHIVE_API_KEY']
FINALITY_CHECKPOINTS_PATH = "/eth/v1/beacon/states/finalized/finality_checkpoints"
INDEX_EPOCH_INCOME_QUEUE_URL = os.environ['INDEX_EPOCH_INCOME_QUEUE_URL']
BEACONCHAIN_SLOT_BLOCK_NUMBER= os.environ['BEACONCHAIN_SLOT_BLOCK_NUMBER']


# Function to get the last updated epoch for which the apr exist for the indexes
def get_epochs(indexes):
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    if len(indexes) == 1:
        INDEX_MAX_EPOCH= f'SELECT indexes, MAX(epoch) FROM Validator_Indexes_APR WHERE indexes = {indexes[0]} GROUP BY indexes;'
    else:
        INDEX_MAX_EPOCH= f'SELECT indexes, MAX(epoch) FROM Validator_Indexes_APR WHERE indexes in {indexes} GROUP BY indexes;'
    
    cursor.execute(INDEX_MAX_EPOCH)
    return {int(validator['indexes']): int(validator['MAX(epoch)']) for validator in cursor.fetchall()}


# Function to convert epoch to block number
def get_indexes():
    q= 'SELECT DISTINCT indexes FROM Validator_Start_Index'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)
     
    return [index['indexes'] for index in cursor.fetchall() if index['indexes']!= None]


# Function to enter indexes into SQL for the epochs
def commit(inputs):
    cursor = connection.cursor()
    cursor.executemany(INDEX_APR_INSERT_QUERY, inputs)
    connection.commit()


def indexAPR(indexes, min_epoch, upper_epoch):
    q= f'SELECT AVG(t2.apr), SUM(t2.earnings), SUM(t2.losses), t2.epoch, t1.indexes FROM Validator_Epoch_Income AS t2 INNER JOIN (SELECT bls_key, epoch, indexes FROM Validator_Indexes WHERE indexes = {indexes} AND epoch BETWEEN {min_epoch} AND {upper_epoch}) AS t1 ON t1.bls_key= t2.bls_key and t1.epoch= t2.epoch AND t2.epoch BETWEEN {min_epoch} AND {upper_epoch} GROUP BY t1.indexes, t2.epoch'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)
    validators= cursor.fetchall()

    return [(validator['indexes'], validator['epoch'], float(validator['AVG(t2.apr)']), float(validator['SUM(t2.earnings)']), float(validator['SUM(t2.losses)'])) for validator in validators]

def validator_indexes(index, validators):    
    
    q = f'SELECT Validator_Epoch_Runner.bls_key, Validator_Epoch_Runner.epoch FROM Validator_Epoch_Runner INNER JOIN (SELECT bls_key FROM Validator_Start_Index WHERE indexes = {index} AND bls_key IN {tuple(validators)}) AS Validator_Start_Index ON Validator_Epoch_Runner.bls_key = Validator_Start_Index.bls_key'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    validators = cursor.fetchall()
    if len(validators) == 0:
        return 0
    
    return min([int(validator['epoch']) for validator in validators])

# Lambda that handles the indexes and epochs till which epochs should be updated.
def data_handler(event, context):

    bodies = [json.loads(record['body']) for record in event['Records']]

    indexes= [body['index'] for body in bodies]

    maxEPOCH= get_epochs(tuple(indexes))

    validators = subgraph.fetch_all_bls_keys_from_subgraph()

    inputs = []

    for index in indexes:
        epoch_upper = validator_indexes(index, validators)
        if not epoch_upper:
            continue
        index_income= indexAPR(index, maxEPOCH.get(index, 0) + 1, epoch_upper)
        index_epoch_income= index_income[:]
        for i in range(1, len(index_income)- 1):
            try:
                if abs(index_income[i][2] - index_income[i-1][2])/index_income[i][2] * 100 > 10:
                    if abs(index_income[i][2] - index_income[i+1][2])/index_income[i][2] * 100 > 10:
                        index_epoch_income.pop(i)
            except:
                pass  
        print(f"Index {index} sending {len(index_epoch_income)} epochs of {len(index_income)}")
        inputs = inputs + index_epoch_income
    
    commit(inputs)
    sqs.delete_sqs_messages(INDEX_EPOCH_INCOME_QUEUE_URL, event)


# Function that sends all the indexes along with the epoch 10 at a time
def queue_handler(event, context):

    epoch_response = request_archive(FINALITY_CHECKPOINTS_PATH)

    if not epoch_response or not epoch_response.get('data'):
        raise Exception('Failed to fetch current epoch')

    current_epoch = epoch_response.get('data', {}).get('finalized', {}).get('epoch')

    if not current_epoch:
        raise Exception('Failed to fetch current epoch')

    current_epoch = int(current_epoch)

    print('Latest epoch:', current_epoch)

    messages= []
    for index in get_indexes():
        
        index_hash = sqs.hash_sqs(str(float(index)))
        messages.append(
            {
                'Id': f'{index_hash}-{current_epoch}',
                'MessageBody': json.dumps({'index': int(index), 'epoch_number': current_epoch})
            }
        )

        if len(messages)== 10:
            sqs.post_sqs_messages(INDEX_EPOCH_INCOME_QUEUE_URL, messages)
            messages= []

    if len(messages)> 0:
        sqs.post_sqs_messages(INDEX_EPOCH_INCOME_QUEUE_URL, messages)
    
            
