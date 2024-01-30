import json
import os

import pymysql

from src.utils import sqs
from src.utils.constants import SLOTS_PER_EPOCH
from src.utils.data_utils import get_main_db_connection, INDEX_INSERT_QUERY
from src.utils.archive import request_archive, get_validator_url
from src.utils import subgraph

connection = get_main_db_connection()
connection.autocommit(True)

API_KEY = os.environ['ARCHIVE_API_KEY']
FINALITY_CHECKPOINTS_PATH = "/eth/v1/beacon/states/finalized/finality_checkpoints"
VALIDATOR_INDEX_QUEUE_URL = os.environ['VALIDATOR_INDEX_QUEUE_URL']
BEACONCHAIN_SLOT_BLOCK_NUMBER= os.environ['BEACONCHAIN_SLOT_BLOCK_NUMBER']


# This is directed towards fetching the indexes to which a validator belongs to
# The lambda runs everyday and assigns the indexes to validators based on various
# index transfer events in the duration from last update. This way the indexes are
# maintained based on sequence of index transfers. This file contains the handler
# function as well as the helper functions.


# Function to get the last updated epoch for which the indexes exist for the bls keys.
# The function returns a dictionary where each bls key refers to the maximum epoch for
# which the indexes have been updated.
def get_epochs(bls_keys):
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    if len(bls_keys) == 1:
        INDEX_MAX_EPOCH= f'SELECT bls_key, MAX(epoch) FROM Validator_Indexes WHERE bls_key = \"{bls_keys[0]}\" GROUP BY bls_key ;'
    else:
        INDEX_MAX_EPOCH= f'SELECT bls_key, MAX(epoch) FROM Validator_Indexes WHERE bls_key in {bls_keys} GROUP BY bls_key ;'
    cursor.execute(INDEX_MAX_EPOCH)
    validators = (cursor.fetchall())
    validator_epochs= {validator['bls_key']: int(validator['MAX(epoch)']) + 1 for validator in validators}
    
    return validator_epochs

# Function to get activation epochs for the specified bls keys
def activation_epochs(bls_keys):
    validator_epoch= request_archive(get_validator_url(), params={"id": bls_keys}).get('data')
    activationEpochs= {validator.get('validator').get('pubkey'): validator.get('validator').get('activation_epoch')
                        for validator in validator_epoch}
    
    return activationEpochs

# Function to convert epoch to block number
def epoch_upper_bound(block):
    
    return int((int(block) - int(BEACONCHAIN_SLOT_BLOCK_NUMBER))/SLOTS_PER_EPOCH)

# Function to enter indexes into SQL for the epochs
def commit(inputs):
        cursor = connection.cursor()
        cursor.executemany(INDEX_INSERT_QUERY, inputs)
        connection.commit()


# Function to calculate the indexes based on transfer of indexes. When there is no transfer from the last
# updated epoch to current epoch, all the epochs are assigned current index to which the validator belongs to. Otherwise
# the validator is assigned indexes based on transfer events.
def get_validator_indexes(bls_keys, lastUpdateEpochs, processing_objects):
    
    validator_indexes= []

    
    for bls_key in bls_keys:
        # For all the bls keys check number of index transfers between lastUpdateEpoch and current index
        epoch_number= processing_objects[bls_key]
        lastUpdateEpoch= int(lastUpdateEpochs[bls_key])
        indexTransfers = subgraph.index_transfers(json.dumps(bls_key), lastUpdateEpoch)
        input= []
        if len(indexTransfers)== 0:
            current_index= subgraph.validator_index(json.dumps(bls_key))
            input= [(bls_key, epoch, current_index) for epoch in range(int(lastUpdateEpoch), int(epoch_number) + 1)]

        else:
            # Look for all the transfer of validators and based on that assign indexes
            indexes= []
            for index in reversed(indexTransfers):
                epochs= [(bls_key, epoch, index['value'].split('-')[0]) for epoch in range(int(lastUpdateEpoch), epoch_upper_bound(index['blockNumber']) + 1)]
                lastUpdateEpoch= epoch_upper_bound(index['blockNumber'])
                indexes= indexes + epochs
            
            # Assigning indexes from last transfer epoch to the current epoch
            lastIndex= indexTransfers[-1]['value'].split('-')[1]
            epochs= [(bls_key, epoch, lastIndex) for epoch in range(int(indexes[-1][1])+ 1, int(epoch_number)+ 1)]
            indexes= indexes + epochs
            input= indexes
        
        validator_indexes= validator_indexes + input
    return validator_indexes

# Lambda that handles the bls keys and epochs till which epochs should be updated. bls keys for which
# indexes have been updated earlier are handled separately and for them indexes are assigned from last epoch
# for which indexes were assigned. For the validators for which indexes were not updated, indexes are assigned
# from activation epoch till the current epoch
def data_handler(event, context):
    bodies = [json.loads(record['body']) for record in event['Records']]
    print("Recieved= ", len(bodies))
    processing_objects = {body['bls_key']: body['epoch_number'] for body in bodies}

    bls_keys= list(processing_objects.keys())
    maxEPOCHS= get_epochs(tuple(bls_keys))
    print("bls_keys= ", bls_keys)
    print("Max Epochs= ", maxEPOCHS)
    print("bls_keys= ", bls_keys)

    bls_keys_with_max_epoch= list(maxEPOCHS.keys())
    
    print("bls_keys_with_max_epoch= ", bls_keys_with_max_epoch)
    bls_keys_without_max_epoch= list(set(bls_keys)- set(bls_keys_with_max_epoch))
    print("bls_keys_without_max_epoch= ", bls_keys_without_max_epoch)

    input_bls_key_with_max_epoch= []
    input_bls_key_without_max_epoch= []

    if len(bls_keys_with_max_epoch)>0:
        input_bls_key_with_max_epoch= get_validator_indexes(bls_keys_with_max_epoch, maxEPOCHS, processing_objects)
        print("input_bls_key_with_max_epoch=", input_bls_key_with_max_epoch)
        
        commit(input_bls_key_with_max_epoch)

    if len(bls_keys_without_max_epoch)>0:
        activationEpochs= activation_epochs(bls_keys_without_max_epoch)
        input_bls_key_without_max_epoch= get_validator_indexes(bls_keys_without_max_epoch, activationEpochs, processing_objects)
        print("input_bls_key_without_max_epoch=", input_bls_key_without_max_epoch)
        commit(input_bls_key_without_max_epoch)

    sqs.delete_sqs_messages(VALIDATOR_INDEX_QUEUE_URL, event)


# Function that sends all the validators along with the epoch 10 at a time
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
    for bls_key in subgraph.fetch_all_bls_keys_from_subgraph():

        bls_key_hash = sqs.hash_sqs(bls_key)
        messages.append(
            {
                'Id': f'{bls_key_hash}-{current_epoch}',
                'MessageBody': json.dumps({'bls_key': bls_key, 'epoch_number': current_epoch})
            }
        )

        if len(messages)== 10:
            sqs.post_sqs_messages(VALIDATOR_INDEX_QUEUE_URL, messages)
            messages= []

    if len(messages)> 0:
        sqs.post_sqs_messages(VALIDATOR_INDEX_QUEUE_URL, messages)


