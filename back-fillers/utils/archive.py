import requests
import os


API_KEY = ""
URL = ""


def request_archive(uri_path, params=None):
    destination_url = URL + uri_path  # Assuming URI_PATH starts with /

    return requests.get(destination_url, params=params, headers={'x-api-key': API_KEY}).json()

def get_validator_url(state='finalized'):
    
    return f'/eth/v1/beacon/states/{state}/validators'

