import boto3
import csv
import json
from botocore.exceptions import ClientError
import time
import pymysql

# Initialize AWS clients
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')
glue_client = boto3.client('glue')
secrets_client = boto3.client('secretsmanager')
rds_data_client = boto3.client('rds-data')
rds_client = boto3.client('rds')


# Amazon SQS URL and AWS Glue Job Name (Replace with your actual names)
SQS_QUEUE_URL = "https://sqs.us-west-1.amazonaws.com/982723143439/DatabaseFilePathQueue.fifo"
GLUE_JOB_NAME = "delta-load-glue-job"
RDS_SECRET_NAME = "arn:aws:secretsmanager:us-west-1:982723143439:secret:rds-secret-V5LSD8"


def lambda_handler(event, context):
    # Log the received event for debugging purposes
    print("Received event:", json.dumps(event))

    # Check if the event contains 'Records' (S3 event)
    if 'Records' in event:
        # Extract S3 event details
        s3_event = event['Records'][0]
        bucket_name = s3_event['s3']['bucket']['name']
        key = s3_event['s3']['object']['key']
        print(f"Bucket: {bucket_name}, Key: {key}")
        
    # Case 2: Event contains 'filePath' (SQS event format)
    elif 'filePath' in event:
        # Extract file path from the event
        file_path = event['filePath']
        print(f"File Path: {file_path}")
        bucket_name, key, folder_name = parse_s3_path(file_path)
    else:
        print("Error: Missing expected fields in event")
        return
    
    # Continue with processing...
    # Analyze the database file in S3
    schema = analyze_database_file(bucket_name, key)

    # Fetch Redshift credentials from AWS Secrets Manager
    rds_credentials = get_rds_credentials()

    # Create a table in Amazon Redshift
    create_rds_table(schema, key, rds_credentials)

    # Trigger AWS Glue Job with file_path as payload
    trigger_glue_job(file_path)


def parse_s3_path(s3_path):
    """Parse the S3 path into bucket, key, and folder_name."""
    parts = s3_path.replace("s3://", "").split("/", 1)
    bucket_name = parts[0]
    key = parts[1]
    folder_name = key.split("/")[0]  # Extract the folder name (first part of the key)
    return bucket_name, key, folder_name

def analyze_database_file(bucket_name, key):
    """Analyze the CSV file in S3 to extract schema."""
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    body = response['Body'].read().decode('utf-8')
    reader = csv.reader(body.splitlines())
    headers = next(reader)

    # Example: Analyze first row to infer data types
    first_row = next(reader)
    schema = []
    for header, value in zip(headers, first_row):
        if value.isdigit():
            data_type = "INTEGER"
        else:
            try:
                float(value)
                data_type = "FLOAT"
            except ValueError:
                data_type = "VARCHAR(255)"
        schema.append({"column_name": header, "data_type": data_type})

    print(f"Extracted schema: {schema}")
    return schema


def get_rds_credentials():
    """Fetch RDS PostgreSQL credentials from AWS Secrets Manager."""
    try:
        response = secrets_client.get_secret_value(SecretId=RDS_SECRET_NAME)
        secret = json.loads(response['SecretString'])
        return {
            "RDS_HOST": secret['host'],
            "RDS_PORT": secret['port'],
            "RDS_DATABASE_NAME": secret['database'],
            "RDS_USER": secret['username'],
            "RDS_PASSWORD": secret['password']  
        }
    except ClientError as e:
        print(f"Error fetching secret from Secrets Manager: {str(e)}")
        raise Exception("Failed to retrieve RDS credentials from Secrets Manager") from e


def create_rds_table(schema, key, rds_credentials):
   
    # Extract table name from the S3 key
    table_name = key.split("/")[-1].replace(".csv", "")
    print(f"Table name to create: {table_name}")
    
    # Construct the SQL CREATE TABLE query
    columns = ", ".join([
        f'"{col["column_name"].lstrip("\ufeff").replace("\"", "\"\"")}" {col["data_type"]}'
        for col in schema
    ])
    #primary_key_column = schema[0]["column_name"].lstrip("\ufeff").replace("\"", "\"\"")

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
    {', '.join([f'`{col["column_name"].lstrip("\ufeff").replace("\"", "\"\"")}` {col["data_type"]}' for col in schema])}
    );
    """
    #PRIMARY KEY (`{primary_key_column}`)
    
    
    print(f"Generated CREATE TABLE query:\n{create_table_query}")
    
    # Connect to the RDS PostgreSQL database using `boto3`
    try:
        print("Attempting to connect to RDS Database...")
       
        
        # Execute the SQL statement

        print("Attempting to connect to RDS MySQL...")
        connection = pymysql.connect(
            host=rds_credentials['RDS_HOST'],
            user=rds_credentials['RDS_USER'],
            password=rds_credentials['RDS_PASSWORD'],
            database=rds_credentials['RDS_DATABASE_NAME'],
            port=rds_credentials['RDS_PORT']
        )

        
        with connection.cursor() as cursor:
            # Execute the CREATE TABLE query
            print("Executing CREATE TABLE query...")
            cursor.execute(create_table_query)
            connection.commit()
            print(f"Table '{table_name}' creation completed successfully.")
       
    except pymysql.MySQLError as e:
        print(f"Error creating table in RDS MySQL: {str(e)}")
        raise   
    
    finally:
        # Close the database connection
        if 'connection' in locals() and connection:
            connection.close()
            print("Database connection closed.")

def trigger_glue_job(file_path):
    """Trigger AWS Glue Job with file path as a parameter."""
    
    # Validate the S3 path format
    if not file_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path format: {file_path}")
    
    print(f"Validated file_path: {file_path}")
    
    try:
        # Trigger the Glue job, passing the file_path as a parameter
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--file_path": file_path
            }
        )
        
        # Print and log the Glue job's response
        print(f"Glue job triggered successfully with JobRunId: {response['JobRunId']}")
        
        # Optionally, return the response for further processing or logging
        return response
    
    except Exception as e:
        # Log and raise any exceptions that occur
        print(f"Error triggering Glue job: {e}")
        raise e  # Reraise the exception for error handling by AWS Lambda