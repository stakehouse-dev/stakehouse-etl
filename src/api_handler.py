from cmath import nan
import json
import pymysql
from operator import itemgetter
from statistics import mean
import requests
from datetime import datetime, timedelta

from src.utils import subgraph
from src.utils.data_utils import get_main_db_connection
from src.utils.archive import request_archive, validator_url, request_validator
from src.utils.apiUtils import form_index_record, get_time_with_lag, top_indexes, get_validators_with_indices


import os

connection = get_main_db_connection()

def fetchValidators(yesterday):
    dbQuery = f'SELECT * FROM Validators.Validator_Daily_APR WHERE dates="{yesterday}"'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(dbQuery)
    return cursor.fetchall()

def validator_slashing(bls_key):

    q = f'SELECT MAX(losses) FROM Validators.Validator_Epoch_Income WHERE bls_key="{bls_key}"'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)
    return cursor.fetchone()['MAX(losses)']

def form_response(body, code):
    return {
        'statusCode': code,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True
        },
        'body': json.dumps(body)
    }

def index_validators(indexes):
    if len(indexes) == 1:
        q= f'SELECT bls_key, indexes FROM Validator_Start_Index WHERE indexes = \"{tuple(indexes)[0]}\"'
    else:
        q= f'SELECT bls_key, indexes FROM Validator_Start_Index WHERE indexes in {tuple(indexes)}'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    validator_indexes = {index: [] for index in indexes}

    for validator in cursor.fetchall():

        validator_indexes[int(validator['indexes'])].append(validator['bls_key'])
    
    return validator_indexes

def validator_epoch_apr(bls_key, epochs):
    q = f'SELECT epoch, apr, earnings FROM Validators.Validator_Epoch_Income WHERE bls_key=\"{bls_key}\" ORDER BY epoch DESC LIMIT {epochs}'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)
    
    validator_epoch_aprs= cursor.fetchall()

    totaldETHEarnings= float(validator_epoch_aprs[0]['earnings'])
     
    validator_epoch_apr= [{'epoch': float(entry['epoch']), 'apr': float(entry['apr'])} for entry in validator_epoch_aprs]

    return validator_epoch_apr, totaldETHEarnings

def validator_apr_between_epochs(bls_key, epoch_lower, epoch_upper):

    q = f'SELECT epoch, earnings FROM Validators.Validator_Epoch_Income WHERE bls_key=\"{bls_key}\" AND epoch BETWEEN {epoch_lower} AND {epoch_upper}  ORDER BY epoch DESC'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)
    
    validator_epoch_aprs= cursor.fetchall()

    if len(validator_epoch_aprs) == 0:
        return []
     
    validator_epoch_apr= [{'epoch': float(entry.get('epoch')), 'earnings': float(entry.get('earnings'))} for entry in validator_epoch_aprs]

    return validator_epoch_apr

def threat_monitoring(bls_keys):
    if len(bls_keys) == 0:
        return []
    if len(bls_keys) == 1:
        q= f'SELECT * FROM Validator_Threat_Monitoring WHERE bls_key = \"{bls_keys[0]}\";'
    else:
        q= f'SELECT * FROM Validator_Threat_Monitoring WHERE bls_key in {bls_keys};'
    
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    validators = {entry['bls_key']: [entry['dETHBacking'],entry['samePosition'],entry['dETHBalance']] for entry in cursor.fetchall()}

    return validators

def validator_indexes(index):
    q= f'SELECT DISTINCT bls_key FROM Validator_Start_Index WHERE indexes= {index}'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    validators= [validator['bls_key'] for validator in cursor.fetchall()]
    
    return validators


def indexAPR(index, epochs):
    q= f'SELECT epoch, apr FROM Validator_Indexes_APR WHERE indexes= {index} ORDER BY epoch DESC LIMIT {epochs}'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)
    validators= cursor.fetchall()
    apr_index = {int(entry['epoch']): float(entry['apr']) for entry in validators}

    OpenIndex= f'SELECT epoch, apr FROM Validator_Indexes_APR WHERE indexes= 0 ORDER BY epoch DESC LIMIT {epochs}'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(OpenIndex)
    validatorsOpenIndex= cursor.fetchall()
    apr_open_index = {int(entry['epoch']): float(entry['apr']) for entry in validatorsOpenIndex}

    return apr_open_index, apr_index

def validator_epoch_apr_blsKeys():
    q= 'SELECT DISTINCT bls_key FROM Validator_Balances'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    return [str(entry['bls_key']) for entry in cursor.fetchall()]

def dETHEarning_Index(index):
    q= f'SELECT SUM(earnings) AS earnings from Validator_Epoch_Income AS epoch_income INNER JOIN (SELECT bls_key as bls_key, t.epoch as epoch from Validator_Indexes AS t INNER Join (select max(epoch) as epoch from Validator_Indexes_APR WHERE indexes= {index}) as tm ON t.epoch = tm.epoch AND t.indexes = {index}) validators ON epoch_income.bls_key = validators.bls_key AND epoch_income.epoch = validators.epoch'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    return float(cursor.fetchone()['earnings'])

def dETHEarning_Slashing_Index_Validators(bls_keys):

    if len(bls_keys) == 1:
        q= f'SELECT bls_key, MAX(earnings), MAX(losses) FROM Validator_Epoch_Income WHERE bls_key = \"{bls_keys[0]}\" GROUP BY bls_key'
    else:
        q= f'SELECT bls_key, MAX(earnings), MAX(losses) FROM Validator_Epoch_Income WHERE bls_key in {bls_keys} GROUP BY bls_key'
    
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)
    validators = [(float(validator['MAX(earnings)']), (float(validator['MAX(losses)']))) for validator in cursor.fetchall()] 

    return [sum(tup) for tup in zip(*validators)]

def dETHEarning_Earnings_Slashing_Validators(bls_keys):

    if len(bls_keys) == 1:
        q= f'SELECT bls_key, MAX(earnings), MAX(losses) FROM Validator_Epoch_Income WHERE bls_key = \"{bls_keys[0]}\" GROUP BY bls_key'
    else:
        q= f'SELECT bls_key, MAX(earnings), MAX(losses) FROM Validator_Epoch_Income WHERE bls_key in {bls_keys} GROUP BY bls_key'
    
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)
    
    validators = {validator['bls_key']: { 'earnings': float(validator['MAX(earnings)']), 'losses': (float(validator['MAX(losses)']))} for validator in cursor.fetchall()}

    return validators

def index_slashing_validators(bls_keys):

    if len(bls_keys) == 1:
        q= f'SELECT bls_key, MAX(losses) FROM Validator_Epoch_Income WHERE bls_key = \"{bls_keys[0]}\" GROUP BY bls_key'
    else:
        q= f'SELECT bls_key, MAX(losses) FROM Validator_Epoch_Income WHERE bls_key in {bls_keys} GROUP BY bls_key'
    
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    return sum([float(validator['MAX(losses)']) for validator in cursor.fetchall()])

def indexes_apr(indexes):

    if len(indexes) == 1:
        q= f'SELECT Validator_Indexes.indexes, Validator_Indexes.apr, Validator_Indexes.earnings FROM Validator_Indexes_APR Validator_Indexes INNER JOIN (SELECT MAX(epoch) as epoch FROM Validator_Indexes_APR WHERE indexes = \"{indexes[0]}\" GROUP BY indexes) maxes on Validator_Indexes.epoch = maxes.epoch;'
    else:
        q= f'SELECT Validator_Indexes.indexes, Validator_Indexes.apr, Validator_Indexes.earnings FROM Validator_Indexes_APR Validator_Indexes INNER JOIN (SELECT indexes, MAX(epoch) as epoch FROM Validator_Indexes_APR WHERE indexes in {indexes} GROUP BY indexes) maxes on Validator_Indexes.epoch = maxes.epoch and Validator_Indexes.indexes = maxes.indexes;'
    
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    validators = {float(entry['indexes']): {'apr': float(entry['apr'])} for entry in cursor.fetchall()}

    if len(indexes) == 1:
        q= f'SELECT SUM(earnings) as earnings, indexes from Validator_Epoch_Income AS epoch_income INNER JOIN (SELECT bls_key as bls_key, t.epoch as epoch, t.indexes as indexes from Validator_Indexes AS t INNER Join (select max(epoch) as epoch from Validator_Indexes_APR WHERE indexes = \"{indexes[0]}\") as tm ON t.epoch = tm.epoch AND t.indexes IN \"{indexes[0]}\") validators ON epoch_income.bls_key = validators.bls_key AND epoch_income.epoch = validators.epoch GROUP BY 2'
    else:
        q= f'SELECT SUM(earnings) as earnings, indexes from Validator_Epoch_Income AS epoch_income INNER JOIN (SELECT bls_key as bls_key, t.epoch as epoch, t.indexes as indexes from Validator_Indexes AS t INNER Join (select max(epoch) as epoch from Validator_Indexes_APR WHERE indexes IN {indexes}) as tm ON t.epoch = tm.epoch AND t.indexes IN {indexes}) validators ON epoch_income.bls_key = validators.bls_key AND epoch_income.epoch = validators.epoch GROUP BY 2'
    
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    for validator in cursor.fetchall():
        validators[float(validator['indexes'])]["earnings"] = float(validator['earnings']) / (10**9)

    return validators

def user_apr(bls_keys, epochs):

    if len(bls_keys) == 1:
        q = f'SELECT epoch, AVG(apr) FROM Validators.Validator_Epoch_Income WHERE bls_key = \"{bls_keys[0]}\" GROUP BY epoch ORDER BY epoch DESC LIMIT {epochs}'
    else:
        q = f'SELECT epoch, AVG(apr) FROM Validators.Validator_Epoch_Income WHERE bls_key in {tuple(bls_keys)} GROUP BY epoch ORDER BY epoch DESC LIMIT {epochs}'
    
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    validator_apr= cursor.fetchall()

    if len(validator_apr) == 0:
        return None

    return {int(validator['epoch']): float(validator['AVG(apr)']) for validator in validator_apr}

def validator_earnings(bls_keys):

    if len(bls_keys) == 1:
        q = f'SELECT bls_key, earnings, losses FROM Validators.Validator_Earnings_Interface WHERE bls_key = \"{bls_keys[0]}\"'
    else:
        q = f'SELECT bls_key, earnings, losses FROM Validators.Validator_Earnings_Interface WHERE bls_key in {tuple(bls_keys)}'
    
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    validator_apr= cursor.fetchall()

    if len(validator_apr) == 0:
        return None

    return {validator['bls_key']: float(validator['earnings']) for validator in validator_apr}, {validator['bls_key']: float(validator['losses']) for validator in validator_apr}

def validator_index_stakehouse(bls_keys):
    if len(bls_keys) == 1:
        q= f'SELECT bls_key, indexes FROM Validator_Start_Index WHERE bls_key = \"{bls_keys[0]}\" GROUP BY bls_key'
    else:
        q= f'SELECT bls_key, indexes FROM Validator_Start_Index WHERE bls_key in {tuple(bls_keys)} GROUP BY bls_key'
    
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    return {validator['bls_key']: int(validator['indexes']) for validator in cursor.fetchall()}

def validator_slot_withdrawals(validator, withdrawal_index_lower, limit):
    q= f'SELECT validator, slot, amount, withdrawal_index FROM Validator_Slot_Withdrawals WHERE validator = {validator} AND withdrawal_index > {withdrawal_index_lower} ORDER BY withdrawal_index ASC LIMIT {limit}'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    return [{'slot': int(validator['slot']), 'amount': int(validator['amount']), 'withdrawal_index': validator['withdrawal_index']} for validator in cursor.fetchall()]

def validator_slot_withdrawals_lower_slot(validator, slot_lower, slot_upper):
    q= f'SELECT validator, slot, amount, withdrawal_index FROM Validator_Slot_Withdrawals WHERE validator = {validator} AND slot BETWEEN {slot_lower} AND {slot_upper}'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    return [{'slot': int(validator['slot']), 'amount': int(validator['amount']), 'withdrawal_index': validator['withdrawal_index']} for validator in cursor.fetchall()]

def validator_slot_withdrawals_lower_upper_slot(validator):
    q= f'SELECT validator, MAX(slot), MIN(slot) FROM Validator_Slot_Withdrawals WHERE validator = {validator}'
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(q)

    validator_slot = cursor.fetchone()

    return  int(validator_slot['MAX(slot)']), int(validator_slot['MIN(slot)'])

def handle_request_leaderboard(event, context):
    yesterday= get_time_with_lag(1)
    validators= fetchValidators(yesterday)
    savETHIndexId= subgraph.fetch_savETHIndexID()

    validators = [validator for validator in validators if savETHIndexId.get(validator['bls_key'])]

    for validator in validators:
        validator['savETHIndex']= savETHIndexId[validator['bls_key']]

    if len(validators) == 0:
        return form_response("No entry", 400)
    validators= sorted(validators, key = itemgetter('savETHIndex'))
    final_data = get_validators_with_indices(validators)
    savETHIndexRecord = [form_index_record(entry) for entry in final_data]

    return form_response(savETHIndexRecord, 200)

def handle_request_topIndexes(event, context):
    yesterday= get_time_with_lag(1)
    validators= fetchValidators(yesterday)
    savETHIndexId= subgraph.fetch_savETHIndexID()

    validators = [validator for validator in validators if savETHIndexId.get(validator['bls_key'])]
    
    for validator in validators:
        validator['savETHIndex']= savETHIndexId[validator['bls_key']]
    
    if len(validators) == 0:
        return form_response("No entry", 400)
    validators= sorted(validators, key = itemgetter('savETHIndex'))
    final_data = get_validators_with_indices(validators)
    savETHIndexRecord = [top_indexes(entry) for entry in final_data]

    bestValidators= {
        'top7Earnings': sorted(savETHIndexRecord, key = itemgetter('earningsAbsolute'), reverse= True),
        'top7Losses': sorted(savETHIndexRecord, key = itemgetter('lossesAbsolute'), reverse= True),
        'top7APR': sorted(savETHIndexRecord, key = itemgetter('aprPerValidator'), reverse= True),
        }

    return form_response(bestValidators, 200)

def handle_request_indexAPRAverage(event, context):

    indexValidator = event.get('queryStringParameters', {}).get('index', None)
    
    if indexValidator is None:
        raise Exception("No Index Sent")

    yesterday= get_time_with_lag(1)
    validators= fetchValidators(yesterday)
    savETHIndexId= subgraph.fetch_savETHIndexID()

    validators = [validator for validator in validators if savETHIndexId.get(validator['bls_key'])]
    
    for validator in validators:
        validator['savETHIndex']= savETHIndexId[validator['bls_key']]
    
    if len(validators) == 0:
        return form_response("No entry", 400)
    
    validators= sorted(validators, key = itemgetter('savETHIndex'))
    final_data = get_validators_with_indices(validators)

    indexes = [top_indexes(entry) for entry in final_data]

    all_index_average_apr= mean([index['aprPerValidator'] for index in indexes])

    if indexValidator not in [i[0] for i in final_data]:
        indexAPR= " "
    else:
        indexAPR= next(index['aprPerValidator'] for index in indexes if index["indexId"] == indexValidator)

    dETHEarnings= dETHEarning_Index(indexValidator)
    apr_average= {
        'indexAPR': indexAPR,
        'allIndexAPRAverage': all_index_average_apr,
        'totaldETHEarned': dETHEarnings
        }

    return form_response(apr_average, 200)

def handle_request_validatorEpochWiseApr(event, context):
    epochs = event.get('queryStringParameters', {}).get('epochs', None)

    if epochs is None:
        raise Exception("No Index Sent")

    bls_key= event.get('queryStringParameters', {}).get('bls_key', None)

    if epochs is None:
        raise Exception("No bls_key Sent")

    if bls_key not in validator_epoch_apr_blsKeys():
        return form_response("Validator BLS Key Doesn't Exist", 400)

    validator_apr, totaldETHEarnings= validator_epoch_apr(bls_key, int(epochs))

    if int(epochs) > len(validator_apr):
        return form_response("Required Epochs Doesn't Exist For The Validator Yet", 400)

    validator_epoch= {
        'validatorEpochWiseApr': validator_apr,
        'totaldETHEarnings': totaldETHEarnings
    }

    return form_response(validator_epoch, 200)

def handle_request_averageIndexAPR(event, context):
    epochs = event.get('queryStringParameters', {}).get('epochs')

    if epochs is None:
        raise Exception("No Index Sent")

    index= event.get('queryStringParameters', {}).get('index')

    if index is None:
        raise Exception("No bls_key Sent")
    
    openIndexAPR, IndexAPR= indexAPR(int(index), int(epochs))
    validator_epoch= {
        'indexAPR': IndexAPR,
        'openIndexAPR': openIndexAPR
    }

    return form_response(validator_epoch, 200)

def handle_request_indexdETHEarned(event, context):

    index= event.get('queryStringParameters', {}).get('index')

    if index is None:
        raise Exception("No bls_key Sent")
    
    
    dETHEarned= {
        'dETHEarned': dETHEarning_Index(index)
    }

    return form_response(dETHEarned, 200)

def handle_request_indexValidators(event, context):
    
    index= event.get('queryStringParameters', {}).get('index')
    if index is None:
        raise Exception("No bls_key Sent")
    
    validators= subgraph.validator_in_index(index)
    bls_keys= validator_indexes(index)
    if len(bls_keys) == 0:
        print(f'No start validator index info for {index}')
        print('Checking if validators were part of LSD index')
        print(validators)
        validators_to_lsd_index= subgraph.validator_lsd(validators)

        for (validator) in validators:
            validator_index= validators_to_lsd_index[validator]
            bls_keys_returned= validator_indexes(validator_index)
            for (bls_pub_key) in bls_keys_returned:
                if validator == bls_pub_key:
                    bls_keys += [validator]

        if len(bls_keys) == 0:
            return form_response('No Validators', 401)

    bls_key_threat_monitoring= threat_monitoring(tuple(bls_keys))

    for bls_key in set(bls_keys)- set(bls_key_threat_monitoring.keys()):
        bls_key_threat_monitoring[bls_key]= [1,1,1]
    
    validator_index= {}
    for bls_key in bls_keys:
        if bls_key in validators:
            validator_index[bls_key] = 1
        else:
            validator_index[bls_key]= 0
    
    dETHEarned= {
        'validators': bls_key_threat_monitoring,
        'validator_indexes': validator_index
    }

    return form_response(dETHEarned, 200)


def handle_request_validatorLSDScore(event, context):
    
    bls_key= event.get('queryStringParameters', {}).get('bls_key')

    if bls_key is None:
        raise Exception("No bls_key Sent")

    VALIDATOR_URL = '/eth/v1/beacon/states/finalized/validators/' + bls_key

    validator= request_archive(VALIDATOR_URL)
    effective_balance= int(validator['data']['validator']['effective_balance'])
    balance= int(validator['data']['balance'])

    if effective_balance < 32000000000:
        validator_score = 2
    elif balance< 32000000000 and balance > 31750000000:
        validator_score = 1
    else:
        validator_score = 0
    
    top_ups= subgraph.validator_topups(bls_key)
    slashing= validator_slashing(bls_key)

    validator= {'validator_score': validator_score, 'top_ups': max(0, int(slashing)- int(top_ups))/ 10 ** 9}

    return form_response(validator, 200)

def handle_request_indexRedemptionRate(event, context):
    
    index= event.get('queryStringParameters', {}).get('index')

    if index is None:
        raise Exception("No bls_key Sent")

    bls_keys= validator_indexes(index)

    if len(bls_keys) == 0:
        return form_response('No Validators', 401)

    dETH, slashing= dETHEarning_Slashing_Index_Validators(tuple(bls_keys))

    top_ups= subgraph.index_topups(bls_keys)

    live_score = (((24 * 10**9) * len(bls_keys)) +  dETH) / (((8 * 10**9) * len(bls_keys)) - float(slashing) + top_ups)
    
    exchange_rate= (((24 * 10**9) * len(bls_keys)) +  dETH) / (((8 * 10**9) * len(bls_keys)))

    if live_score/exchange_rate > 1.25:
        colour = 'red'
    elif live_score/exchange_rate > 1.1:
        colour= 'Amber'
    else:
        colour= 'green'

    redemptionRate= {
        'redemptionRate': live_score,
        'colour': colour
    }
    return form_response(redemptionRate, 200)

def handle_request_savETHIndexNames(event, context):

    solo_stakers= subgraph.solo_staker_indexes()
    stakers= subgraph.staker_indexes()
    
    liquid_indexes= subgraph.liquid_index()

    for index in liquid_indexes:
        solo_stakers[index]= stakers[str(index)]

    savETHIndex= {
        'savETHIndex': solo_stakers
    }
    return form_response(savETHIndex, 200)


def handle_request_mevWatchInfo(event, context):

    start= (datetime.utcnow() - timedelta(hours = 24)).strftime('%s')

    end= datetime.utcnow().strftime('%s')
    url = "https://www.mevwatch.info/api/blockStats"

    payload = json.dumps({
    "startTime": int(start),
    "endTime": int(end)
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    return form_response(response.text, 200)

def handle_request_indexLeaderboard(event, context):

    indexes, validator_indexes= subgraph.staker_index()
    print("Indexes")
    print(indexes)
    print("Validator_Indexes")
    print(validator_indexes)
    if len(indexes) == 0:
        return form_response("No Indexes", 400)

    index_apr= indexes_apr(tuple(indexes))

    print("Index APR")
    print(index_apr.keys())
    validators= index_validators(indexes)

    print("Validators")
    print(validators.keys())

    for index in index_apr.keys():
        print(f"index = {index}")
        index_apr[int(index)]['bls_keys']= validators[index]
        if index in validator_indexes:
            index_apr[int(index)]['validators'] = 0

        else:
            index_apr[int(index)]['validators']= 1

    return form_response(index_apr, 200)

'''
API for user Income.
:param user: ECDSA of the user
:param epochs: Number epochs for which APR is needed
:returns: Dictionary of user APR, rewards, validators and slippage

This API gives everything needed for the User income in protected LP staking
'''

def handle_request_userIncome(event, context):

    # User and epochs are API parameters
    user= event.get('queryStringParameters', {}).get('user', "").lower()
    epochs= event.get('queryStringParameters', {}).get('epochs', None)

    if not user:
        return form_response('Invalid User', 400)
    
    if not epochs or int(epochs)< 0:
        return form_response('Invalid Epochs', 400)

    # Validators to which the user has invested
    validators= subgraph.user_validators(user)
    bls_keys = list(validators.keys())
    if len(bls_keys) == 0:
        return form_response("No Validators", 400)
    try:
        epoch_earnings = user_apr(bls_keys, epochs)
    except Exception as exception:
        return form_response("Error In Earnings", 400)

    validator_indexes= validator_index_stakehouse(bls_keys)

    if not epoch_earnings:
        return form_response("Not Enough Epochs", 400)

    bls_key_threat_monitoring= threat_monitoring(tuple(bls_keys))

    for bls_key in set(bls_keys)- set(bls_key_threat_monitoring.keys()):
        bls_key_threat_monitoring[bls_key]= [1,1,1]
    
    dETHEarnings, losses = validator_earnings(bls_keys)

    payoff_rate = {}
    for validator in dETHEarnings.keys():
        validators[validator]= validators[validator]/24
        dETHEarnings[validator] = dETHEarnings[validator] * (validators[validator])
        losses[validator] = losses[validator] * (validators[validator])
        if (validators[validator] != 0):
            payoff_rate[validator] = ((8 * validators[validator]) - (losses[validator] / 1000000000)) / (8 * validators[validator])
        else:
            payoff_rate[validator] = 0

    dETHEarned= sum(dETHEarnings.values()) / 10 ** 9

    exchange_earnings= 1 + (dETHEarned/ sum(validators.values()))

    user_income= {
        'epoch_apr': epoch_earnings,
        'dETHEarned': dETHEarned,
        'validators': bls_key_threat_monitoring,
        'slippage': exchange_earnings,
        'nav': sum(list(payoff_rate.values()))/ len(list(payoff_rate.keys())),
        'validator_earnings': validators,
        'payoff_rate': payoff_rate,
        'dETHEarnings': dETHEarnings,
        'validator_indexes': validator_indexes
    }


    return form_response(user_income, 200)


def handle_request_indexValidators_open_index(event, context):
    
    index= 0
    try:
        validators= subgraph.validator_in_index(index)
    except:
        return form_response('No Validators', 400)

    if len(validators) == 0:
        return form_response('No Validators', 401)

    try:
        bls_key_threat_monitoring= threat_monitoring(tuple(validators))
    except:
        return form_response('No Threat Monitoring', 400)
    for bls_key in set(validators)- set(bls_key_threat_monitoring.keys()):
        bls_key_threat_monitoring[bls_key]= [1,1,1]
    
    dETHEarned= {
        'validators': bls_key_threat_monitoring
    }

    return form_response(dETHEarned, 200)

def handle_request_indexRedemptionRate_open_index(event, context):
    
    index= 0

    if index is None:
        raise Exception("No bls_key Sent")

    try:
        bls_keys= subgraph.validator_in_index(index)
    except:
        return form_response("No Validators", 400)

    if len(bls_keys) == 0:
        return form_response('No Validators', 401)

    dETH, slashing= dETHEarning_Slashing_Index_Validators(tuple(bls_keys))

    top_ups= subgraph.index_topups(bls_keys)

    live_score = (((24 * 10**9) * len(bls_keys)) +  dETH) / (((8 * 10**9) * len(bls_keys)) - float(slashing) + top_ups)
    
    exchange_rate= (((24 * 10**9) * len(bls_keys)) +  dETH) / (((8 * 10**9) * len(bls_keys)))

    if live_score/exchange_rate > 1.25:
        colour = 'red'
    elif live_score/exchange_rate > 1.1:
        colour= 'Amber'
    else:
        colour= 'green'

    redemptionRate= {
        'redemptionRate': live_score,
        'colour': colour
    }
    return form_response(redemptionRate, 200)

def handle_request_userIncomeMEV(event, context):

    if event is None:
        print("Recieved None Event")

    user= event.get('queryStringParameters', {}).get('user', "").lower()

    print(f"User Is- {user}")
    
    if not user:
        return form_response('Invalid User', 400)

    bls_keys, indexes, validator_slot, payouts= subgraph.user_mev_batch(user)

    if not bls_keys:
        return form_response("No Validators", 400)
    
    validator_indexes= validator_index_stakehouse(bls_keys)
    
    bls_key_threat_monitoring= threat_monitoring(tuple(bls_keys))

    for bls_key in set(bls_keys)- set(bls_key_threat_monitoring.keys()):
        bls_key_threat_monitoring[bls_key]= [1,1,1]
    
    validator_earnings= dETHEarning_Earnings_Slashing_Validators(tuple(bls_keys))

    top_ups= subgraph.validator_topups_mev(bls_keys)

    redemption_rates= {}
    sETH_token= {}
    for bls_key in bls_keys:
        redemption_rates[bls_key] =  (24 + (validator_earnings[bls_key]['earnings'] / 10 ** 9) ) / ((8) - (validator_earnings[bls_key]['losses'] / 10 ** 9)  + top_ups[bls_key])
        sETH_token[bls_key]= ((24 + (validator_earnings[bls_key]['earnings'] / 10 ** 9) ) / 8) * validator_slot[bls_key]

    user_income= {
        'indexes': indexes,
        'validators': bls_key_threat_monitoring,
        'redemption_rate': redemption_rates,
        'validator_slot': validator_slot,
        'payouts': payouts,
        'validator_indexes': validator_indexes,
        'sETH': sETH_token
    }

    return form_response(user_income, 200)


def handle_request_userIncomeNodeRunner(event, context):

    user= event.get('queryStringParameters', {}).get('user', "").lower()

    if not user:
        return form_response('Invalid User', 400)

    bls_keys, indexes, wallets = subgraph.user_node_runner(user)

    if not bls_keys:
        return form_response("No Validators", 400)

    payouts = subgraph.user_income_node_runners(wallets)
    
    validator_indexes= validator_index_stakehouse(bls_keys)
    
    bls_key_threat_monitoring= threat_monitoring(tuple(bls_keys))

    for bls_key in set(bls_keys)- set(bls_key_threat_monitoring.keys()):
        bls_key_threat_monitoring[bls_key]= [1,1,1]
    
    validator_earnings= dETHEarning_Earnings_Slashing_Validators(tuple(bls_keys))

    top_ups= subgraph.validator_topups_mev(bls_keys)

    redemption_rates= {}
    sETH_token= {}
    validator_slot= {}
    for bls_key in bls_keys:
        validator_slot[bls_key]= 4 - (validator_earnings[bls_key]['losses'] / 10 ** 9)  + top_ups.get(bls_key,0)
        redemption_rates[bls_key] =  (24 + (validator_earnings[bls_key]['earnings'] / 10 ** 9) ) / ((8) - (validator_earnings[bls_key]['losses'] / 10 ** 9)  + top_ups.get(bls_key, 0))
        sETH_token[bls_key]= ((24 + (validator_earnings[bls_key]['earnings'] / 10 ** 9) ) / 8) * 4

    user_income= {
        'indexes': len(indexes),
        'validators': bls_key_threat_monitoring,
        'redemption_rate': redemption_rates,
        'validator_slot': validator_slot,
        'payouts': payouts,
        'validator_indexes': validator_indexes,
        'sETH': sETH_token
    }

    return form_response(user_income, 200)

def handle_request_validatorSlotWithdrawals(event, context):

    user= event.get('queryStringParameters', {}).get('validator', "")
    withdrawal_index_lower = event.get('queryStringParameters', {}).get('withdrawal_index_lower', 0)
    default_limit = event.get('queryStringParameters', {}).get('limit', 1)
    
    if not user or not withdrawal_index_lower or not default_limit:
        return form_response('Invalid Limit', 400)
    
    withdrawals = validator_slot_withdrawals(int(user), int(withdrawal_index_lower), int(default_limit))

    return form_response(withdrawals, 200)


'''
Withdrawal APIs
These API Functions Serve Those Who Want To Get Withdrawals

'''


def handle_request_validatorWithdrawalsLowerSlot(event, context):

    user= event.get('queryStringParameters', {}).get('validator', "")
    slot_lower = event.get('queryStringParameters', {}).get('slot_lower', "")
    slot_upper = event.get('queryStringParameters', {}).get('slot_upper', "")
    
    if not user or not slot_lower or not slot_upper:
        return form_response('Invalid Limit', 400)
    
    withdrawals = validator_slot_withdrawals_lower_slot(int(user), int(slot_lower), int(slot_upper))

    return form_response(withdrawals, 200)

def handle_request_validatorWithdrawalsLowerUpperSlot(event, context):

    user= event.get('queryStringParameters', {}).get('validator', "")
    
    if not user:
        return form_response('Invalid User', 400)
    
    withdrawals = validator_slot_withdrawals_lower_upper_slot(user)

    return form_response(withdrawals, 200)

def handle_request_ponBlsKey(event, context):
    user= event.get('queryStringParameters', {}).get('validator', "")
    
    if not user:
        return form_response('Invalid Validator', 400)
    
    validator, status_code = request_validator(validator_url(user))
    
    if status_code != 200:
        return form_response("Failed Validator", status_code)
    
    if validator['data']['validator']['effective_balance'] == '32000000000' and validator['data']['validator']['slashed'] == False:
        return form_response("Validator Eligible", 200)
    
    else:
        return form_response("Validator Not Eligible", 200)
    

'''

DeFi Llama API

'''

def handle_request_lsdWiseAPR(event, context):
    
    ## lsd_index_ticker_mapping= {lsd_index (int): lsd_ticker} for 
    ## LSD Indexes In Smart Contract For The Protocol

    lsd_index_ticker_mapping = subgraph.staker_indexes()

    if len(lsd_index_ticker_mapping.keys()) == 0:
        return form_response("No Indexes", 204)

    # lsd_apr =   {
    #                 lsd_index (float) : {
    #                     apr:
    #                     earnings:
    #                 }
    #             }

    lsd_apr= indexes_apr(tuple(lsd_index_ticker_mapping.keys()))

    stakehouse_lsd = {}

    for lsd in lsd_index_ticker_mapping.keys():
        
        stakehouse_lsd[int(lsd)] = {}
        stakehouse_lsd[int(lsd)]['Ticker'] = lsd_index_ticker_mapping[lsd]

        # For LSDs which are not active, send earnings and APR zero
        stakehouse_lsd[int(lsd)]['Earnings'] = lsd_apr.get(float(lsd), {}).get("earnings", 0)
        stakehouse_lsd[int(lsd)]['APR'] = lsd_apr.get(float(lsd), {}).get("apr", 0)

    return form_response(stakehouse_lsd, 200)


'''
Validator Earnings Between Two Epochs
@dev TODO: Make It With Optional Flags And Sunset handle_request_validatorEpochWiseApr for Frontend
'''

def handle_request_validatorAprEpochs(event, context):

    epoch_start = event.get('queryStringParameters', {}).get('epoch_start', None)

    if epoch_start is None:
        return form_response("No Epoch Start Sent", 400)

    bls_key= event.get('queryStringParameters', {}).get('bls_key', None)

    if bls_key is None:
        return form_response("No BLS Key", 400)
    
    # If epoch_end is not sent, take epoch_upper as current epoch
    epoch_end = event.get('queryStringParameters', {}).get('epoch_end', None)

    if epoch_end is None:
        url= '/eth/v1/beacon/states/finalized/finality_checkpoints'
        try:
            finalized_state_response = request_archive(url)
            epoch_end = finalized_state_response.get('data').get('finalized').get('epoch')
        except Exception as E:
            return form_response(f"epoch_end not sent. failed to get current epoch", 500)

    validator_apr = validator_apr_between_epochs(bls_key, int(epoch_start), int(epoch_end))

    return form_response(validator_apr, 200)

    
