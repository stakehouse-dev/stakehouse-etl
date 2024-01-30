import pymysql
import os
from datetime import datetime, timedelta

from src.utils import subgraph, sqs
from src.utils.data_utils import get_main_db_connection, get_apr_db_connection, APR_INSERT_QUERY
from src.utils.financials import calc_apr
from src.utils.logging import logs

connection = get_main_db_connection()
connection.autocommit(True)

VALIDATOR_APR_QUEUE_URL = os.environ['VALIDATOR_APR_QUEUE_URL']
STAKEHOUSE_GRAPH_URL = os.environ['STAKEHOUSE_GRAPH_URL']
MINIMUM_EPOCH_LEADERBOARD = int(os.environ.get('MINIMUM_EPOCH_LEADERBOARD', 9))

def validator_income(bls_key):
    # DESC order enforces most recent data
    q = f'SELECT earnings, losses FROM Validators.Validator_Epoch_Income WHERE bls_key=\"{bls_key}\" ORDER BY epoch DESC LIMIT 1575'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    validator_income= [{"earnings": int(validator['earnings']), "losses": int(validator['losses'])} for validator in cursor.fetchall()]
    if len(validator_income) < MINIMUM_EPOCH_LEADERBOARD:
        return None, None, None
    
    return validator_income[0]['earnings'] - validator_income[-1]['earnings'], validator_income[0]['losses']- validator_income[-1]['losses'], len(validator_income)
    

def data_handler(event, context):
    logger = logs()
    try:
        bls_keys = [record['body'] for record in event['Records']]

    except:
        logger.error('Failed Validators')
        raise Exception

    print('Number of keys received:', len(bls_keys))

    logger.info('Number of keys received:' + str(len(bls_keys)))

    try:

        apr_connection = get_apr_db_connection()
        cursor = apr_connection.cursor()
        now = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
    
    except Exception:

        logger.error('Failed APR')
        raise Exception

    entries= []

    for bls_key in bls_keys:

        try:
            earnings, losses, epochs = validator_income(bls_key)
            if earnings == None or losses == None:
                print(f"Skipping BLS Key {bls_key}, Minimum Epochs {MINIMUM_EPOCH_LEADERBOARD} Not Available")
                continue
            apr = calc_apr(earnings, epochs)
            entry= (bls_key, now, earnings, losses, apr)
            entries.append(entry)
        
        except:

            logger.error('Failed BLS KEY' + str(bls_key))
            raise Exception

    cursor.executemany(APR_INSERT_QUERY, entries)
    apr_connection.commit()

    logger.handlers.clear()
    sqs.delete_sqs_messages(VALIDATOR_APR_QUEUE_URL, event)


def queue_handler(event, context):
    for bls_key in subgraph.fetch_all_bls_keys_from_subgraph():
        sqs.post_sqs_message(VALIDATOR_APR_QUEUE_URL, bls_key)

