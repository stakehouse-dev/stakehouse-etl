import json
import os

import pymysql

from src.utils import sqs
from src.utils.data_utils import get_main_db_connection
from src.utils import subgraph

connection = get_main_db_connection()
connection.autocommit(True)

VALIDATOR_HIGHEST_EARNINGS_QUEUE_URL = os.environ['VALIDATOR_HIGHEST_EARNINGS_QUEUE_URL']

def validator_earnings(bls_keys):
    if len(bls_keys) == 1:
            q = f'''
            SELECT
            Validator_Income.bls_key,
            Validator_Income.epoch,
            Validator_Income.earnings as MaxEarnings,
            Validator_Income.losses as MaxLosses
            FROM Validator_Epoch_Income Validator_Income
            INNER JOIN (SELECT 
                        bls_key,
                        MAX(epoch) as maxEpoch
                        FROM Validator_Epoch_Income
                        WHERE bls_key = \"{tuple(bls_keys)[0]}\"
                        GROUP BY bls_key) Validator_Epoch ON 
            Validator_Epoch.bls_key = Validator_Income.bls_key AND Validator_Epoch.maxEpoch = Validator_Income.epoch
            GROUP BY Validator_Income.bls_key
    '''
    else:    
        q = f'''
                SELECT
                Validator_Income.bls_key,
                Validator_Income.epoch,
                Validator_Income.earnings as MaxEarnings,
                Validator_Income.losses as MaxLosses
                FROM Validator_Epoch_Income Validator_Income
                INNER JOIN (SELECT 
                            bls_key,
                            MAX(epoch) as maxEpoch
                            FROM Validator_Epoch_Income
                            WHERE bls_key IN {tuple(bls_keys)}
                            GROUP BY bls_key) Validator_Epoch ON 
                Validator_Epoch.bls_key = Validator_Income.bls_key AND Validator_Epoch.maxEpoch = Validator_Income.epoch
                GROUP BY Validator_Income.bls_key
        '''
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    return [(validator['bls_key'], validator['MaxEarnings'], validator['MaxLosses']) for validator in cursor.fetchall()]

def commit(inputs):
    q= f'INSERT INTO Validator_Earnings_Interface (bls_key, earnings, losses) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE earnings= VALUES(earnings), losses= VALUES(losses)'

    cursor = connection.cursor(pymysql.cursors.DictCursor)

    cursor.executemany(q, inputs)

    connection.commit()

# Lambda that handles the indexes and epochs till which epochs should be updated.
def data_handler(event, context):
    bodies = [json.loads(record['body']) for record in event['Records']]
    print("Recieved= ", len(bodies))
    bls_keys = [body['bls_key'] for body in bodies]
    inputs= validator_earnings(bls_keys)

    commit(inputs)

    sqs.delete_sqs_messages(VALIDATOR_HIGHEST_EARNINGS_QUEUE_URL, event)


# Function that sends all the indexes along with the epoch 10 at a time
def queue_handler(event, context):

    bls_keys= list(subgraph.fetch_all_bls_keys_from_subgraph())
    messages= []
    for bls_key in bls_keys:
        # Priniting BLS Key as I want to do some debugging. Will remove
        bls_key_hash = sqs.hash_sqs(bls_key)
        messages.append(
            {
                'Id': f'{bls_key_hash}-{len(messages)}',
                'MessageBody': json.dumps({'bls_key': bls_key})
            }
        )

        if len(messages)== 10:
            sqs.post_sqs_messages(VALIDATOR_HIGHEST_EARNINGS_QUEUE_URL, messages)
            messages= []

    if len(messages)> 0:
        sqs.post_sqs_messages(VALIDATOR_HIGHEST_EARNINGS_QUEUE_URL, messages)