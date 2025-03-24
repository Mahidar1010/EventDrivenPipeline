import pandas as pd

# Path to your Parquet file in S3
s3_parquet_path = 's3://edp-target-120569637987/converted/'

# Read the Parquet file into a Pandas DataFrame
df = pd.read_parquet(s3_parquet_path, engine='pyarrow')

# Show the first 5 rows of the DataFrame
print(df)  # To see all records in the DataFrame

