# Glue Pyhton Script

import sys
import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import re
from datetime import datetime

# AWS S3 Client
s3 = boto3.client('s3')

# S3 Bucket Details
BUCKET_NAME = "stock-market-180294199258-us-east-1"
SOURCE_PREFIX = "transformed/"
DESTINATION_PREFIX = "parquet/"

def list_s3_files(bucket, prefix):
    """List all JSON files in the S3 transformed folder."""
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.json')]
    return files

def extract_date_from_filename(filename):
    """Extract only the YYYY-MM-DD date part from filenames using regex."""
    match = re.match(r"(\d{4}-\d{2}-\d{2})", filename)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y-%m-%d")
        except ValueError as e:
            print(f"Error parsing date from filename {filename}: {e}")
            return None
    print(f"Skipping file {filename} due to incorrect format.")
    return None

def convert_to_parquet(json_files):
    """Convert JSON to Parquet and store in partitioned format."""
    for file in json_files:
        # Retrieve the JSON file from S3
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=file)
        data = pd.read_json(obj['Body'])

        # Rename the "exchange" column to "stock_exchange" to match Athena table schema
        if 'exchange' in data.columns:
            data.rename(columns={'exchange': 'stock_exchange'}, inplace=True)

        # Extract filename and parse date for partitioning
        filename = file.split('/')[-1]
        date = extract_date_from_filename(filename)
        if date is None:
            print(f"Skipping file {filename} due to date parsing error.")
            continue

        # Create partition path using year, month, week, and day
        partition_path = f"{DESTINATION_PREFIX}year={date.year}/month={date.month}/week={date.isocalendar()[1]}/day={date.day}/"

        # Convert DataFrame to Parquet format using PyArrow
        table = pa.Table.from_pandas(data)
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer)

        # Convert pyarrow.Buffer to bytes for S3 upload
        bytes_data = buffer.getvalue().to_pybytes()

        # Build the full S3 key for the output Parquet file
        parquet_file = f"{partition_path}{filename.replace('.json', '.parquet')}"
        s3.put_object(Bucket=BUCKET_NAME, Key=parquet_file, Body=bytes_data)

        print(f"Converted {filename} to Parquet and stored at {parquet_file}")

def main():
    print("Starting Glue Python Shell Job...")
    json_files = list_s3_files(BUCKET_NAME, SOURCE_PREFIX)
    if json_files:
        convert_to_parquet(json_files)
        print("Parquet conversion completed successfully!")
    else:
        print("No JSON files found in transformed folder!")

if __name__ == "__main__":
    main()
