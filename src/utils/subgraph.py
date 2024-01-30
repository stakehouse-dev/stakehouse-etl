import requests
import os
import json
from collections import Counter

STAKEHOUSE_GRAPH_URL = os.environ['STAKEHOUSE_GRAPH_URL']
STAKEHOUSE_LIQUID_INDEX_GRAPH_URL = os.environ['STAKEHOUSE_LIQUID_INDEX_GRAPH_URL']
BEACONCHAIN_SLOT_BLOCK_NUMBER= os.environ['BEACONCHAIN_SLOT_BLOCK_NUMBER']

STAKEHOUSE_GRAPH_INDEX_V2_URL= os.environ['STAKEHOUSE_GRAPH_INDEX_V2_URL']
SUPPORT_V1_INDEX = int(os.environ['SUPPORT_V1_INDEX'])


def fetch_all_bls_keys_from_subgraph():
    q = """
        {
            stakehouseAccounts(where: {lifecycleStatus: "3"}, first:1000) {
                id
            }
        }
    """

    resp = requests.post(STAKEHOUSE_GRAPH_URL, json={'query': q}).json()

    if not resp.get('data', False):
        raise Exception('Failed Subgraph Query')

    data = resp['data']

    return [d['id'] for d in data['stakehouseAccounts']]

def fetch_savETHIndexID():
    q = """
        {
            knots(where: {rageQuit: false}, first: 1000) {
                id
                savETHIndexId
            }
        }
    """

    resp = requests.post(STAKEHOUSE_GRAPH_URL, json={'query': q}).json()

    if not resp.get('data', False):
        raise Exception('Failed Subgraph Query')

    data = resp['data']
    return {str(d['id']): d['savETHIndexId'] for d in data['knots']}

def index_transfers(bls_key, lastUpdateEpoch):
    q = """
        {
        events(orderBy: blockNumber, orderDirection: desc, where:{
        key_in: ["KNOT_TRANSFERRED_FROM_INDEX"],
        blsPubKeyForKnot:""" + bls_key + """
        blockNumber_gt:""" + str((int(lastUpdateEpoch) * 32) + int(BEACONCHAIN_SLOT_BLOCK_NUMBER)) + """
        }){
        blockNumber
        value
        }
        }
        """

    resp = requests.post(STAKEHOUSE_GRAPH_URL, json={'query': q}).json()
    return resp['data']['events']

def validator_index(bls_key):
    q = """
            {
            knots(where:{
            id:""" + bls_key + """
            }){
            savETHIndexId
            }
            }
            """

    resp = requests.post(STAKEHOUSE_GRAPH_URL, json={'query': q}).json()
    savETHIndex= resp.get('data').get('knots')[0].get('savETHIndexId', None)
    
    if not savETHIndex:
        raise Exception('savETHIndex failed to call')
    return savETHIndex

def validator_in_index(index):
    q = """
            {
            knots(where:{
            savETHIndexId:""" + str(index) + """,
            rageQuit: false},
            first:1000
            ){
                id
            }
            }
            """

    resp = requests.post(STAKEHOUSE_GRAPH_URL, json={'query': q}).json()
    savETHIndex= resp.get('data').get('knots', None)
    
    if savETHIndex== None:
        raise Exception('savETHIndex failed to call')
    return [str(d['id'])for d in savETHIndex]

def validator_topups(bls_key):
    q = """
            {
            stakehouseAccount(where:{
            id:""" + bls_key + """
            }){
            totalETHTopUps
            }
            }
            """

    resp = requests.post(STAKEHOUSE_GRAPH_URL, json={'query': q}).json()
    savETHIndex= resp.get('data').get('stakehouseAccount')[0].get('totalETHTopUps', None)
    
    if not savETHIndex:
        raise Exception('savETHIndex failed to call')
    return savETHIndex

def validator_topups(bls_key):
    q = """
            {
            stakehouseAccount(
            id:""" + json.dumps(bls_key) + """
            ){
            totalETHForSLOTInQueue
    		totalETHForSLOTSentToDepositContract
            }
            }
            """

    resp = requests.post(STAKEHOUSE_GRAPH_URL, json={'query': q}).json()
    savETHIndex= int(resp.get('data').get('stakehouseAccount').get('totalETHForSLOTInQueue', None)) + int(resp.get('data').get('stakehouseAccount').get('totalETHForSLOTSentToDepositContract', None))
    
    return savETHIndex

def solo_staker_indexes():
    q = """
            {
            savETHIndexes(first: 1000) {
                indexId
                label
            }
            }
            """

    resp = requests.post(STAKEHOUSE_GRAPH_URL, json={'query': q}).json()
    savETHIndex= resp.get('data').get('savETHIndexes', None)
    if not savETHIndex:
        raise Exception('savETHIndex failed to call')
    return {i['indexId']: i['label'] for i in savETHIndex}

def liquid_index():
    q = """
            {
            lsdvalidators(first: 1000, where:{
                currentIndex_not : 0}) {
                currentIndex
                
            }
            }
            """
    if SUPPORT_V1_INDEX == 1:
        resp = requests.post(STAKEHOUSE_LIQUID_INDEX_GRAPH_URL, json={'query': q}).json()
        stakehouseAccount = resp.get('data').get('lsdvalidators')

        resp2 = requests.post(STAKEHOUSE_GRAPH_INDEX_V2_URL, json={'query': q}).json()
        stakehouseAccount2 = resp2.get('data').get('lsdvalidators')

        indexes = list(Counter([int(i['currentIndex']) for i in stakehouseAccount]).keys())
        
        indexes2 = list(Counter([int(i['currentIndex']) for i in stakehouseAccount2]).keys())
        
        return indexes + indexes2

    else:

        resp2 = requests.post(STAKEHOUSE_GRAPH_INDEX_V2_URL, json={'query': q}).json()
        stakehouseAccount2 = resp2.get('data').get('lsdvalidators')
        
        indexes2 = list(Counter([int(i['currentIndex']) for i in stakehouseAccount2]).keys())

        return indexes2

def index_validators(index):
    q = """
            {
            savETHIndex(id:""" + str(index) + """
            ){
                numberOfKnots
            }
            }
            """

    resp = requests.post(STAKEHOUSE_GRAPH_URL, json={'query': q}).json()
    savETHIndex= resp.get('data').get('savETHIndex').get('numberOfKnots', None)
    
    if not savETHIndex:
        raise Exception('savETHIndex failed to call')
    return int(savETHIndex)

def index_topups(bls_keys):
    q = """
            {
            stakehouseAccounts(first:1000,
            where:{
                knotMetadata_: {
                id_in:""" + json.dumps(bls_keys) + """
                }
            }

            ){
            totalETHTopUps
            }
            }
            """

    resp = requests.post(STAKEHOUSE_GRAPH_URL, json={'query': q}).json()
    stakehouseAccount = resp.get('data').get('stakehouseAccounts')
    top_ups = sum([float(i['totalETHTopUps']) for i in stakehouseAccount])
    
    return int(top_ups)

def validator_topups_mev(bls_keys):
    q = """
            {
            stakehouseAccounts(first:1000,
            where:{
                knotMetadata_: {
                id_in:""" + json.dumps(bls_keys) + """
                }
            }

            ){
            totalETHForSLOTInQueue
    		totalETHForSLOTSentToDepositContract
            knotMetadata{
                id
            }
            }
            }
            """

    resp = requests.post(STAKEHOUSE_GRAPH_URL, json={'query': q}).json()
    stakehouseAccount = resp.get('data').get('stakehouseAccounts')
    top_ups = {validator['knotMetadata']['id']: (float(validator['totalETHForSLOTInQueue']) + float(validator['totalETHForSLOTSentToDepositContract'] )) for validator in stakehouseAccount}
    
    return top_ups

def index_minted():
    q = """
                        {
            stakehouseAccounts(first:1000){
                id
                totalDETHMinted
            }
            }
            """
    resp = requests.post(STAKEHOUSE_GRAPH_URL, json={'query': q}).json()
    stakehouseAccount = resp.get('data').get('stakehouseAccounts')
    indexes = {i['id']: i['totalDETHMinted'] for i in stakehouseAccount}
    
    return indexes

def staker_index():
    q = """
           {
            liquidStakingNetworks (first:1000) {
                lsdIndex
            }
            lsdvalidators(first: 1000, where:{
                currentIndex_not : 0}) {
                currentIndex
                
            }
            }
            """
    
    if SUPPORT_V1_INDEX == 1:
        resp = requests.post(STAKEHOUSE_LIQUID_INDEX_GRAPH_URL, json={'query': q}).json()
        resp2 = requests.post(STAKEHOUSE_GRAPH_INDEX_V2_URL, json={'query': q}).json()

        savETHIndex= resp.get('data').get('liquidStakingNetworks', None)
        savETHIndex2= resp2.get('data').get('liquidStakingNetworks', None)

        stakehouseAccount = resp.get('data').get('lsdvalidators')
        stakehouseAccount2 = resp2.get('data').get('lsdvalidators')

        indexes = list(Counter([int(i['currentIndex']) for i in stakehouseAccount]).keys())
        indexes2 = list(Counter([int(i['currentIndex']) for i in stakehouseAccount2]).keys())

        return [int(i['lsdIndex']) for i in savETHIndex] + [int(i['lsdIndex']) for i in savETHIndex2], indexes + indexes2
    
    else:
        resp2 = requests.post(STAKEHOUSE_GRAPH_INDEX_V2_URL, json={'query': q}).json()
        
        savETHIndex2= resp2.get('data').get('liquidStakingNetworks', None)

        stakehouseAccount2 = resp2.get('data').get('lsdvalidators')

        indexes2 = list(Counter([int(i['currentIndex']) for i in stakehouseAccount2]).keys())

        return [int(i['lsdIndex']) for i in savETHIndex2], indexes2


def staker_indexes():
    q = """
           {
            liquidStakingNetworks (first:1000) {
                lsdIndex
                ticker
            }
            }

        """

    if SUPPORT_V1_INDEX == 1:
        resp = requests.post(STAKEHOUSE_LIQUID_INDEX_GRAPH_URL, json={'query': q}).json()
        savETHIndex= resp.get('data').get('liquidStakingNetworks', None)

        resp2 = requests.post(STAKEHOUSE_GRAPH_INDEX_V2_URL, json={'query': q}).json()
        savETHIndex2 = resp2.get('data').get('liquidStakingNetworks')
        
        return {i['lsdIndex']: i['ticker'] for i in savETHIndex + savETHIndex2}

    else:

        resp2 = requests.post(STAKEHOUSE_GRAPH_INDEX_V2_URL, json={'query': q}).json()
        savETHIndex2 = resp2.get('data').get('liquidStakingNetworks')
        
        return {i['lsdIndex']: i['ticker'] for i in savETHIndex2}

def user_validators(user):
    q = """
           {
            protectedBatches(first:1000, where: {
                listOfLiquidityProviderAddresses_contains_nocase: [""" + json.dumps(user) + """]
                vaultLPToken_:{
                lifecycleStatus: "MINTED_DERIVATIVES"
                }
            }){
                lsdValidator {
                id
                }
            liquidityProviders(first:100, where:{
            lpAddress: \"""" + user + """\"
            } 
            ) {
            amount
            }
            }
            lptokens(where:{
                liquidityProviders_:{
                lpAddress: \"""" + user + """\"
                }
                lifecycleStatus: "MINTED_DERIVATIVES"
                tokenType: "PROTECTED_STAKING_LP"
            }) {
                blsPublicKey
                liquidityProviders(where:{
                lpAddress: \"""" + user + """\"
                }) {
                lpAddress
                amount
                }
            }
            }
            """
    
    resp = requests.post(STAKEHOUSE_GRAPH_INDEX_V2_URL, json={'query': q}).json()
    pool_validators= resp.get('data').get('protectedBatches', [])
    lp_validators= resp.get('data').get('lptokens', [])    

    validators= {validator['lsdValidator']['id']: (sum([float(i['amount']) for i in validator['liquidityProviders']]) / 10 ** 18) for validator in pool_validators if len(validator['lsdValidator'])> 0}
    for validator in lp_validators:
        validators[validator['blsPublicKey']] = (sum([float(i['amount']) for i in validator['liquidityProviders']]) / 10 ** 18)
    return validators

def user_mev_batch(user):

    q = """
           {
            feesAndMevBatches
            (where:{
                liquidityProviders_:{
                lpAddress: \"""" + user + """\"
                }
                vaultLPToken_:{
                lifecycleStatus: "MINTED_DERIVATIVES"
                }
            }) 
            {
                blsPublicKey
                lsdValidator{
                liquidStakingManager
                }
                liquidityProviders
                (where:{
                lpAddress: \"""" + user + """\"
                }) 
                {
                amount
                withdrawn
                }
            }
              payouts(first:1000, where: {
                type: "GIANT_POOL",
                user: \"""" + user + """\"
            }) {
                amount
            }
            }
            """

    resp2 = requests.post(STAKEHOUSE_GRAPH_INDEX_V2_URL,
                          json={'query': q}).json()

    feesAndMevBatches = resp2.get('data').get('feesAndMevBatches', None)

    if not feesAndMevBatches:
        return None, None, None, None

    validators = [validator['blsPublicKey'] for validator in feesAndMevBatches]
    validator_slot = {validator['blsPublicKey']: (sum([float(
        i['amount']) for i in validator['liquidityProviders']]) / 10 ** 18) for validator in feesAndMevBatches}
    indexes = set([validator['lsdValidator']['liquidStakingManager']
                  for validator in feesAndMevBatches])

    payouts = sum([float(validator['amount'])
                  for validator in resp2.get('data').get('payouts', [])]) / (10 ** 18)

    return validators, len(set(indexes)), validator_slot, payouts


def user_node_runner(user):
    
    q = """
            {
        smartWallets(where: {
            nodeRunner_: {
            id: \"""" + user + """\"
            }
        }) {
            id
        }
        
        nodeRunners(where: {
            id: \"""" + user + """\"
            validators_: {
            status: "MINTED_DERIVATIVES"
            }
        }) {
            validators(where: {
            status: "MINTED_DERIVATIVES"
            }) {
            id
            liquidStakingManager
            }
        }
        }
        """

    resp2 = requests.post(STAKEHOUSE_GRAPH_INDEX_V2_URL, json={'query': q}).json()
    nodeRunners= resp2.get('data').get('nodeRunners', None)

    if not nodeRunners:
        return None, None, None

    validators = [validator['id'] for validator in nodeRunners[0]['validators']]
    
    indexes= set([validator['liquidStakingManager'] for validator in nodeRunners[0]['validators']])

    wallets = [validator['id'] for validator in resp2.get('data').get('smartWallets', None)]

    return validators, indexes, wallets

def user_income_node_runners(bls_keys):

    q = """
            {
            payouts(where: {
                type: "NODE_OPERATOR",
                user_in: """ + json.dumps(bls_keys) + """
            }) {
                amount
            }
            }
        """

    resp2 = requests.post(STAKEHOUSE_GRAPH_INDEX_V2_URL, json={'query': q}).json()

    payouts= resp2.get('data').get('payouts', None)

    if not payouts:
        return None

    payout = sum([float(validator['amount']) for validator in payouts]) / (10 ** 18)

    return payout

def validator_lsd(bls_keys):
    q = """
            {
            lsdvalidators(where:{
                id_in: """ + json.dumps(bls_keys) + """
            }){
                id
                smartWallet{
                    liquidStakingNetwork{
                    lsdIndex
                    }
                }
            }
            }
        """

    resp2 = requests.post(STAKEHOUSE_GRAPH_INDEX_V2_URL, json={'query': q}).json()

    validators= resp2.get('data', {}).get('lsdvalidators', [])


    validator_index = {validator.get('id'): int(validator.get('smartWallet').get('liquidStakingNetwork').get('lsdIndex')) for validator in validators}

    return validator_index