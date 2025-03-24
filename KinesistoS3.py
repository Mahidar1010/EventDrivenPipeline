import json
import boto3
import base64
import uuid
import time

# AWS clients
s3_client = boto3.client('s3')

# S3 Bucket and Folder details
BUCKET_NAME = 'edp-source-120569637987'
FOLDER_PATH = 'inbound/'

def lambda_handler(event, context):
    # Iterate over each record from the Kinesis event
    for record in event['Records']:
        # Decode Kinesis record payload from Base64
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        
        # Since the data is in CSV format, we can directly write it to S3
        # Create a unique filename based on timestamp or UUID
        timestamp = int(time.time())
        file_name = f"{FOLDER_PATH}userdata_{timestamp}_{str(uuid.uuid4())}.csv"
        
        # Store the CSV data in S3
        try:
            response = s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=payload,  # Directly store the CSV content
                ContentType='text/csv'  # Set ContentType to 'text/csv'
            )
            print(f"Successfully stored data in S3: {response}")
        except Exception as e:
            print(f"Error storing data in S3: {str(e)}")

    return {
        'statusCode': 200,
        'body': json.dumps('Successfully processed records')
    }
