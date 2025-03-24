import boto3
import json
import time
import requests
import pandas as pd
from botocore.exceptions import ClientError
import io

# AWS Config
REGION_NAME = "us-east-1"
STREAM_NAME = "UserData-Stream"

# Initialize AWS clients
kinesis_client = boto3.client('kinesis', region_name=REGION_NAME)
secrets_client = boto3.client('secretsmanager', region_name=REGION_NAME)

# Function to retrieve API Key from AWS Secrets Manager
def get_api_key(secret_name):
    try:
        # Retrieve secret value from Secrets Manager
        response = secrets_client.get_secret_value(SecretId=secret_name)

        # Check if the secret is in string format or binary format
        if 'SecretString' in response:
            secret = response['SecretString']
            secret_json = json.loads(secret)
            return secret_json.get("API_KEY", None)
        else:
            return None

    except ClientError as e:
        print(f"Error fetching API key from Secrets Manager: {e}")
        return None

# API Config
API_URL = 'https://api.api-ninjas.com/v1/randomuser'
API_KEY = get_api_key('API_Credentials')

if not API_KEY:
    print("Error: API Key not found in Secrets Manager.")
    exit(1)

# Function to fetch user data from API
def fetch_user_data():
    try:
        response = requests.get(API_URL, headers={'X-Api-Key': API_KEY})
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}, {response.text}")
            return None
    except Exception as e:
        print(f"Exception: {e}")
        return None

# Function to convert JSON data to CSV format
def convert_json_to_csv(data):
    """Convert the JSON data to CSV format."""
    df = pd.DataFrame([data])  # Convert JSON to DataFrame
    csv_buffer = io.StringIO()  # Use StringIO to simulate file in memory
    df.to_csv(csv_buffer, index=False)  # Write DataFrame to CSV
    return csv_buffer.getvalue()  # Return CSV as string

# Function to send data to Kinesis
def send_to_kinesis(csv_data, username):
    try:
        kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=csv_data,
            PartitionKey=username  # Using 'username' as partition key
        )
        print(f"Data sent to Kinesis for {username}.")
    except Exception as e:
        print(f"Failed to send data: {e}")

# Main loop to fetch and send data
record_count = 0
while record_count < 2:
    user_data = fetch_user_data()
    if user_data:
        record_count += 1
        # Convert JSON data to CSV
        csv_data = convert_json_to_csv(user_data)
        username = user_data.get("username", "default_username")
        send_to_kinesis(csv_data, username)
        print(f"Record {record_count}/5 sent")
    time.sleep(5)  # 5-second interval to control data flow

print("Data ingestion completed.")
