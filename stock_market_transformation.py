import json
import boto3
import datetime
import re

# AWS S3 Client
s3_client = boto3.client('s3')

# Constants
SOURCE_BUCKET = "stock-market-180294199258-us-east-1"
SOURCE_FOLDER = "inbound/"
DEST_FOLDER = "transformed/"

def format_market_cap(value):
    """Convert market cap to human-readable format (Trillions, Billions, Millions)."""
    if value >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.2f}T"
    elif value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    elif value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    else:
        return f"{value:.2f}"

def convert_unix_to_date(unix_timestamp):
    """Convert UNIX timestamp to human-readable format (YYYY-MM-DD)."""
    return datetime.datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d')

def transform_data(json_data, price_type):
    """
    Apply transformations to stock data.
    In addition to converting dates and formatting market cap,
    add a new column 'price_type' to indicate whether the data represents an opening or closing price.
    """
    transformed_data = []
    for item in json_data:
        transformed_item = item.copy()
        transformed_item['updated'] = convert_unix_to_date(item['updated'])
        transformed_item['market_cap'] = format_market_cap(item['market_cap'])
        transformed_item['price_type'] = price_type  # New column added based on file name
        transformed_data.append(transformed_item)
    return transformed_data

def lambda_handler(event, context):
    """AWS Lambda function to transform stock market data."""
    source_key = None  # Declare variable at the beginning

    try:
        # Validate event structure
        if 'Records' not in event or not event['Records']:
            raise ValueError("Event structure is incorrect or missing 'Records'.")

        s3_event = event['Records'][0].get('s3', {})
        source_bucket = s3_event.get('bucket', {}).get('name')
        source_key = s3_event.get('object', {}).get('key')

        if not source_bucket or not source_key:
            raise ValueError("Missing S3 bucket name or object key in event.")

        # Ignore files not in the "inbound/" folder
        if not source_key.startswith(SOURCE_FOLDER):
            print(f"Ignoring file {source_key}, not in '{SOURCE_FOLDER}' folder.")
            return {
                "statusCode": 200,
                "message": f"Ignored file: {source_key}"
            }

        print(f"Processing file: {source_key}")

        # Determine price_type from file name by checking for "opening" or "closing"
        lower_key = source_key.lower()
        if "opening" in lower_key:
            price_type = "opening"
        elif "closing" in lower_key:
            price_type = "closing"
        else:
            price_type = "unknown"

        # Read JSON file from S3
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        json_data = json.loads(response['Body'].read().decode('utf-8'))

        # Transform Data: include date conversion, market cap formatting, and price_type addition.
        transformed_data = transform_data(json_data, price_type)

        # Define new S3 path for transformed data
        new_file_key = source_key.replace(SOURCE_FOLDER, DEST_FOLDER)

        # Save Transformed JSON to S3
        s3_client.put_object(
            Bucket=SOURCE_BUCKET,
            Key=new_file_key,
            Body=json.dumps(transformed_data, indent=2),
            ContentType='application/json'
        )

        print(f"Successfully transformed data and stored at {new_file_key}")

        return {
            "statusCode": 200,
            "message": f"Successfully transformed data and stored at {new_file_key}"
        }

    except json.JSONDecodeError:
        error_message = f"Error processing file {source_key}: Invalid JSON format."
        print(error_message)
        return {"statusCode": 500, "error": error_message}

    except Exception as e:
        error_message = f"Error processing file {source_key if source_key else 'unknown'}: {str(e)}"
        print(error_message)
        return {"statusCode": 500, "error": error_message}
