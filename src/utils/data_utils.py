import pymysql
import os

SQL_ENDPOINT = os.environ['SQL_ENDPOINT']
USER = os.environ['DB_USER']
PASS = os.environ['DB_PASS']
DATABASENAME = os.environ['EXTRACT_DATABASE_NAME']
DAY_DATABASE_NAME = os.environ['DAY_DATABASE_NAME']

VALIDATOR_INSERT_QUERY = 'INSERT INTO Validator_Balances (bls_key, epoch, balance) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE balance= VALUES(balance);'
APR_INSERT_QUERY = 'INSERT INTO Validator_Daily_APR(bls_key, dates, earnings, losses, apr) values (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE earnings= VALUES(earnings), losses= VALUES(losses), apr= VALUES(apr);'
INDEX_INSERT_QUERY = 'INSERT INTO Validator_Indexes (bls_key, epoch, indexes) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE indexes= VALUES(indexes);'
APR_EPOCH_INSERT_QUERY = 'INSERT INTO Validator_Epoch_Income(bls_key, epoch, earnings, losses, apr, epochs_since_active) values (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE earnings= VALUES(earnings), losses= VALUES(losses), apr= VALUES(apr), epochs_since_active= VALUES(epochs_since_active);'
INDEX_APR_INSERT_QUERY= 'INSERT INTO Validator_Indexes_APR (indexes, epoch, apr, earnings ,losses) VALUES (%s,%s,%s, %s, %s) ON DUPLICATE KEY UPDATE apr= VALUES(apr);'
VALIDATOR_EPOCH_RUNNER_INSERT_QUERY = 'INSERT INTO Validator_Epoch_Runner(bls_key, epoch) VALUES (%s, %s) ON DUPLICATE KEY UPDATE epoch = VALUES(epoch)'

def get_apr_db_connection():
    return pymysql.connect(
        host=SQL_ENDPOINT,
        user=USER,
        passwd=PASS,
        db=DAY_DATABASE_NAME
    )


def get_main_db_connection():
    return pymysql.connect(
        host=SQL_ENDPOINT,
        user=USER,
        passwd=PASS,
        db=DATABASENAME
    )
