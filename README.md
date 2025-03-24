# Scalability Event Driven Pipeline for User Engagement Analysis

## Project Overview
This project implements a scalable, event-driven data pipeline using AWS services for real-time user engagement analysis. Data is extracted from an external API using Amazon Kinesis Data Streams, processed with AWS Lambda functions, stored in Amazon S3, and transformed into Parquet format for efficient querying using Amazon Athena. Visualizations are created using Amazon QuickSight.

## 🛠Technologies Used
- **AWS Services:** Kinesis Data Streams, Lambda, S3, SNS, SQS, CloudWatch, Glue, Athena, QuickSight, EventBridge
- **API:** Ninjas API
- **Python Libraries:** Boto3, Pandas, PyArrow

## Project Architectur
![Architecture](EDP.png)

## Extract: Real-Time Data Ingestion
- Data is ingested from an external API using Python.
- AWS Kinesis Data Streams captures real-time data.
- A Lambda function (**Lambda_1**) reads data from Kinesis and converts it from JSON to CSV, storing it in an S3 source bucket.

## Transform: Data Processing
- An SNS topic sends notifications when new CSV files are uploaded to the source bucket.
- SQS acts as a queue to receive messages from SNS.
- A Lambda function (**Lambda_2**) processes the CSV files, converts them into Parquet format using Pandas and PyArrow, and stores them in the target S3 bucket.

## Load: Data Storage
- AWS Glue Crawler reads the Parquet files from the target S3 bucket, infers the schema, and updates the Glue Data Catalog.
- A Lambda function (**Lambda_3**) triggers the Glue Crawler whenever a Parquet file is added.

## Query and Visualize
- Athena queries the data using SQL.
- QuickSight visualizes insights using dashboards and charts.
  
## Dashboard
![QuickSight_Dashboard](https://github.com/Mahidar1010/EventDrivenPipeline/blob/main/Screenshot%202025-03-24%20005722.png).

## 🛡Error Handling and Monitoring
- SQS Dead Letter Queue (DLQ) stores failed Lambda messages.
- CloudWatch triggers an alarm for DLQ activity and notifies via SNS.
- EventBridge detects Glue Crawler failures and sends notifications using SNS.

## SQL Queries for Analysis
1. **Count of Users by Company:**
```sql
SELECT username, REGEXP_EXTRACT(email, '@([^\.]+)') AS company_name, COUNT(*) OVER (PARTITION BY REGEXP_EXTRACT(email, '@([^\.]+)')) AS user_count
FROM "user-data".converted;
```
2. **User Count by Age Group:**
```sql
SELECT CASE WHEN age BETWEEN 18 AND 25 THEN '18-25'
            WHEN age BETWEEN 26 AND 35 THEN '26-35'
            WHEN age BETWEEN 36 AND 50 THEN '36-50'
            ELSE '50+' END AS age_group,
       COUNT(*) AS user_count
FROM "user-data".converted
GROUP BY age_group;
```

## Setup Instructions
1. Clone the repository:
```bash
git clone https://github.com/Mahidar1010/EventDrivenPipeline.git
```
2. Install required Python packages:
```bash
pip install boto3 pandas pyarrow
```
3. Set up AWS resources using the AWS Management Console.
4. Configure Lambda functions with required permissions and add triggers (SNS, SQS, and S3 events).
5. Create Glue Crawler and Glue Data Catalog.
6. Configure Athena to query Glue Data Catalog tables.
7. Create visualizations using Amazon QuickSight.

## Contributing
Feel free to raise issues or contribute through pull requests.

## Contact
For any questions, reach out to [Mahidar Reddy Putta](https://www.linkedin.com/in/mahidar-reddy-putta-8258b2203/) on LinkedIn.
