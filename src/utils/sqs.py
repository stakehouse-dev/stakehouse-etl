import boto3
from hashlib import sha256

client = boto3.client('sqs')

def post_sqs_message(queue_url, message):
    client.send_message(
        QueueUrl=queue_url,
        MessageBody=message
    )


def post_sqs_messages(queue_url, messages):
    client.send_message_batch(
        QueueUrl=queue_url,
        Entries=messages
    )

def delete_sqs_messages(queue_url, event):
    entries = [
        {
            'Id': entry['messageId'], 'ReceiptHandle': entry['receiptHandle']
        } for entry in event['Records']
    ]

    client.delete_message_batch(
        QueueUrl=queue_url,
        Entries=entries
    )

def hash_sqs(bls_key):
    return sha256(bls_key.encode('utf-8')).hexdigest()
