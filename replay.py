import boto3
import json
from botocore.exceptions import BotoCoreError, ClientError

# AWS Clients
kinesis_client = boto3.client('kinesis', region_name='us-east-1')
lambda_client = boto3.client('lambda', region_name='us-east-1')

# AWS Configurations
KINESIS_STREAM_NAME = 'user-data-stream'
SHARD_ID = 'shardId-000000000000'
LAMBDA_FUNCTION_NAME = 'KinesisToS3Processor'

# Fetch Shard Iterator
def get_shard_iterator():
    try:
        response = kinesis_client.get_shard_iterator(
            StreamName=KINESIS_STREAM_NAME,
            ShardId=SHARD_ID,
            ShardIteratorType='TRIM_HORIZON'  # Start from the earliest record
        )
        return response['ShardIterator']
    except (BotoCoreError, ClientError) as e:
        print(f"Error fetching shard iterator: {e}")
        return None

# Read Records from Kinesis
def read_records_from_kinesis(shard_iterator):
    records = []
    try:
        while shard_iterator:
            response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=100)
            if 'Records' in response and response['Records']:
                records.extend(response['Records'])
            shard_iterator = response.get('NextShardIterator')
            if not response['Records']:
                break
    except (BotoCoreError, ClientError) as e:
        print(f"Error reading records from Kinesis: {e}")
    return records

# Invoke Lambda with Records
def invoke_lambda_with_records(records):
    for record in records:
        try:
            event = {'Records': [record]}
            response = lambda_client.invoke(
                FunctionName=LAMBDA_FUNCTION_NAME,
                InvocationType='Event',
                Payload=json.dumps(event)
            )
            print(f"Lambda invoked successfully: {response['StatusCode']}")
        except (BotoCoreError, ClientError) as e:
            print(f"Error invoking Lambda: {e}")

# Main Execution
shard_iterator = get_shard_iterator()
if shard_iterator:
    records = read_records_from_kinesis(shard_iterator)
    if records:
        invoke_lambda_with_records(records)
        print(f"✅ Successfully invoked Lambda with {len(records)} records.")
    else:
        print("No records to process.")
else:
    print("Failed to get shard iterator.")
