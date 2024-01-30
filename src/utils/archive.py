import requests
import os


API_KEY = os.environ['ARCHIVE_API_KEY']
URL = os.environ['CONSENSUS_HOST']
SEPOLIA_URL = os.environ['SEPOLIA_URL']


def request_archive(uri_path, params=None):
    destination_url = URL + uri_path  # Assuming URI_PATH starts with /

    return requests.get(destination_url, params=params, headers={'x-api-key': API_KEY}).json()

def get_validator_url(state='finalized'):
    
    return f'/eth/v1/beacon/states/{state}/validators'

def request_execution_payload(slot):
    destination_url = URL +  f'/eth/v2/beacon/blocks/{slot}' # Assuming URI_PATH starts with
    blocks_slot= requests.get(destination_url, headers={'x-api-key': API_KEY})
    if blocks_slot.status_code == 404:
        return None
    else:
        return blocks_slot.json().get('data', {}).get('message', {}).get('body', {}).get('execution_payload', {}).get('withdrawals')
    
def request_execution_payload_sepolia(slot):
    destination_url = SEPOLIA_URL +  f'/eth/v2/beacon/blocks/{slot}' # Assuming URI_PATH starts with
    blocks_slot= requests.get(destination_url, headers={'x-api-key': API_KEY})
    if blocks_slot.status_code == 404:
        return None
    else:
        return blocks_slot.json().get('data', {}).get('message', {}).get('body', {}).get('execution_payload', {}).get('withdrawals')

def validator_url(validator, state='finalized'):

    return URL + f'/eth/v1/beacon/states/{state}/validators/{validator}'

def request_validator(uri_path):

    validator = requests.get(uri_path, headers={'x-api-key': API_KEY})

    if validator.status_code == 200:
        return validator.json(), validator.status_code
    else:
        return None, validator.status_code