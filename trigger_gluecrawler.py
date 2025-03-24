import boto3
import json

# Initialize AWS clients
glue_client = boto3.client('glue')

# Define your Glue Crawler name
CRAWLER_NAME = 'user-data'

def lambda_handler(event, context):
    print("Received event:", json.dumps(event, indent=2))
    
    try:
        # Start the Glue Crawler
        response = glue_client.start_crawler(Name=CRAWLER_NAME)
        print(f"Glue Crawler '{CRAWLER_NAME}' started successfully: {response}")
    except Exception as e:
        print(f"Error starting Glue Crawler: {e}")
        raise e
        
    return {
        'statusCode': 200,
        'body': json.dumps('Glue Crawler triggered successfully!')
    }
