import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

# AWS Config
s3_client = boto3.client('s3')

SOURCE_BUCKET_NAME = 'edp-source-120569637987'
SOURCE_FOLDER_PATH = 'inbound/'
TARGET_BUCKET_NAME = 'edp-target-120569637987'
TARGET_FOLDER_PATH = 'converted/'
TARGET_PARQUET_FILE = f"{TARGET_FOLDER_PATH}combined_data.parquet"

def lambda_handler(event, context):
    print("Received event:", json.dumps(event, indent=2))

    # Process each record from SQS
    for record in event.get('Records', []):
        try:
            # Extract and parse the SQS message body
            sqs_message_body = json.loads(record['body'])
            
            # Extract the actual SNS message
            sns_message = json.loads(sqs_message_body.get('Message', '{}'))
            
            if 'Records' not in sns_message or len(sns_message['Records']) == 0:
                print("No S3 records found in message. Skipping.")
                continue

            # Extract S3 event details
            s3_event = sns_message['Records'][0]
            bucket_name = s3_event.get('s3', {}).get('bucket', {}).get('name')
            object_key = s3_event.get('s3', {}).get('object', {}).get('key')

            if not bucket_name or not object_key:
                print("Missing bucket name or object key. Skipping.")
                continue

            print(f"Processing file: {object_key} from bucket: {bucket_name}")

            # Validate if the object is in the correct source folder
            if bucket_name != SOURCE_BUCKET_NAME or not object_key.startswith(SOURCE_FOLDER_PATH):
                print(f"Skipping file as it is not from the source folder {SOURCE_FOLDER_PATH}")
                continue

            # Process CSV to Parquet
            process_csv_to_parquet(object_key)

        except Exception as e:
            print(f"Error processing record: {str(e)}")

    return {'statusCode': 200, 'body': 'Successfully processed all messages'}

def process_csv_to_parquet(object_key):
    try:
        # Download CSV from S3
        download_path = f"/tmp/{object_key.split('/')[-1]}"
        s3_client.download_file(SOURCE_BUCKET_NAME, object_key, download_path)
        print(f"Downloaded {object_key} to {download_path}")

        # Read CSV into DataFrame
        df_new = pd.read_csv(download_path)
        print(f"New CSV Data:\n{df_new.head()}")

        # Check if the Parquet file exists in the target bucket
        try:
            s3_client.head_object(Bucket=TARGET_BUCKET_NAME, Key=TARGET_PARQUET_FILE)
            print("Existing Parquet file found. Merging with new data...")

            # Download and read existing Parquet data
            response = s3_client.get_object(Bucket=TARGET_BUCKET_NAME, Key=TARGET_PARQUET_FILE)
            parquet_data = response['Body'].read()
            existing_df = pd.read_parquet(io.BytesIO(parquet_data))

            # Combine existing and new data
            final_df = pd.concat([existing_df, df_new], ignore_index=True)
        except s3_client.exceptions.ClientError:
            print("No existing Parquet file found. Creating a new one.")
            final_df = df_new

        # Convert to Parquet
        buffer = io.BytesIO()
        final_df.to_parquet(buffer, engine='pyarrow', index=False)
        buffer.seek(0)
        
        # Upload to S3
        s3_client.put_object(Bucket=TARGET_BUCKET_NAME, Key=TARGET_PARQUET_FILE, Body=buffer.getvalue())
        print(f"Successfully updated Parquet file in {TARGET_BUCKET_NAME}/{TARGET_PARQUET_FILE}")

    except Exception as e:
        print(f"Error during CSV to Parquet conversion: {e}")
        raise
