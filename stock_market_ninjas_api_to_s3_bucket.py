import json
import boto3
import requests
import datetime

s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
sns_client = boto3.client('sns')

BUCKET_NAME = "stock-market-180294199258-us-east-1"
FOLDER_NAME = "inbound/"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:180294199258:stockDataSourceDE"
SECRET_NAME = "stockAPIKey"  

symbols = ['AAPL', 'NVDA', 'MSFT', 'AMZN', 'GOOGL', 'META', 'TSLA', 'AVGO', 'ADBE', 'NFLX',
           'IBM', 'ORCL', 'CRM', 'ACN', 'SAP', 'TSM', 'TXN', 'QCOM', 'SONY', 'BABA']

PRICE_URL = 'https://api.api-ninjas.com/v1/stockprice?ticker={}'
MARKETCAP_URL = 'https://api.api-ninjas.com/v1/marketcap?ticker={}'

def get_api_key():
    """Retrieve API key from AWS Secrets Manager (stored as key-value pair)."""
    try:
        secret_response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(secret_response['SecretString'])
        return secret.get('api_key')  
    except Exception as e:
        raise Exception(f"Failed to retrieve API key from Secrets Manager: {str(e)}")

def fetch_stock_data(api_key):
    """Fetch stock price and market cap for each symbol."""
    headers = {'X-Api-Key': api_key}
    stock_data = []

    for symbol in symbols:
        try:
            price_response = requests.get(PRICE_URL.format(symbol), headers=headers)
            price_data = price_response.json() if price_response.status_code == 200 else None

            marketcap_response = requests.get(MARKETCAP_URL.format(symbol), headers=headers)
            marketcap_data = marketcap_response.json() if marketcap_response.status_code == 200 else None

            if price_data:
                stock_info = price_data
                stock_info['market_cap'] = marketcap_data.get('market_cap', 'N/A') if marketcap_data else 'N/A'
                stock_data.append(stock_info)
            else:
                print(f"Error fetching data for {symbol}: Price Status {price_response.status_code}, Market Cap Status {marketcap_response.status_code}")

        except Exception as e:
            print(f"Error processing {symbol}: {str(e)}")

    return stock_data

def get_file_name():
    """Generate a filename based on date, day, and time slot (before/after)."""
    now = datetime.datetime.utcnow()  
    et_time = now - datetime.timedelta(hours=5) 
    
    date_str = et_time.strftime('%Y-%m-%d') 
    day_str = et_time.strftime('%a').upper()
    
    hour = et_time.hour
    time_slot = "opening" if hour < 12 else "closing"

    return f"{FOLDER_NAME}{date_str}_{day_str}_{time_slot}.json"

def upload_to_s3(data):
    """Upload JSON data to S3."""
    try:
        file_name = get_file_name()
        
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        
        return file_name
    except Exception as e:
        raise Exception(f"Failed to upload data to S3: {str(e)}")

def send_sns_notification(subject, message):
    """Send SNS notification."""
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
    except Exception as e:
        print(f"Failed to send SNS notification: {str(e)}")

def lambda_handler(event, context):
    """AWS Lambda handler function."""
    try:
        api_key = get_api_key()
        if not api_key:
            raise Exception("API key is missing in Secrets Manager.")

        stock_data = fetch_stock_data(api_key)

        if not stock_data:
            raise Exception("No stock data retrieved from API.")

        file_name = upload_to_s3(stock_data)

        message = f"Stock data successfully fetched and uploaded to S3 bucket '{BUCKET_NAME}' in file '{file_name}'."
        send_sns_notification("Stock Data Load - SUCCESS", message)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': message, 'file': file_name})
        }

    except Exception as e:
        error_message = f"Lambda execution failed: {str(e)}"
        print(error_message)
        send_sns_notification("Stock Data Load - FAILURE", error_message)

        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }
