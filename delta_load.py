import sys
import boto3
import json
import pandas as pd
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
import logging
import pymysql
import io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
dynamodb_client = boto3.client('dynamodb')
rds_data_client = boto3.client('rds-data')
rds_client = boto3.client('rds')


RDS_SECRET_NAME = "arn:aws:secretsmanager:us-west-1:982723143439:secret:rds-secret-V5LSD8"
SNS_TOPIC_ARN = "arn:aws:sns:us-west-1:982723143439:DeltaLoadCompletionNotification"
DYNAMO_DB_TABLE_NAME = "DeltaLoadTracker"

def main():
    args = getResolvedOptions(sys.argv, ['file_path'])
    file_path = args['file_path']
    
    print(f"Processing file at: {file_path}") 

    if not file_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path format: {file_path}. Ensure it's a valid S3 URI.")

    bucket_name, key, folder_name = parse_s3_path(file_path)
    logger.info(f"Processing file at: {file_path}, Bucket: {bucket_name}, Key: {key}")

    rds_credentials = get_rds_credentials()
    df = read_s3_file_to_dataframe(bucket_name, key)
    
    table_name = key.split("/")[-1].replace(".csv", "")
    if not check_rds_table(table_name, rds_credentials):
        logging.error(f"Table {table_name} not found in the database. Exiting job.")
        return


    df_cleaned = clean_data(df)
    table_name = key.split("/")[-1].replace(".csv", "")

    upload_to_rds(df_cleaned, table_name, rds_credentials)
    
    logger.info(f"Successfully inserted data for {table_name}")

    update_dynamodb(key, folder_name)

    logger.info(f"Successfully processed file {file_path} and uploaded data to RDS.")
    
    send_sns_notification(table_name, file_path)
    
    logger.info("SNS notification sent successfully.")
    

def read_s3_file_to_dataframe(bucket_name, key):
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    body = response['Body'].read().decode('utf-8')

    df = pd.read_csv(io.StringIO(body))
    for index, row in df.iterrows():
        if row.isnull().any():
            message = (
                f"Row with index {index} in the file '{key}' "
                f"from bucket '{bucket_name}' contains null values.\n\n"
                f"Row data: {row.to_dict()}"
            )
            subject = f"Null Values Found in File {key}"
            
            try:
                response = boto3.client('sns').publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=message,
                    Subject=subject
                )
                logger.info(f"SNS Notification sent for null row at index {index}. Response: {response}")
            except ClientError as e:
                logger.error(f"Error sending SNS notification for row with null values: {e}")
    
    return df


    
def parse_s3_path(s3_path):
    parts = s3_path.replace("s3://", "").split("/", 1)
    bucket_name = parts[0]
    key = parts[1]
    folder_name = key.split("/")[0]  # Extract the folder name (first part of the key)
    return bucket_name, key, folder_name
    
def clean_data(df):
    return df.dropna().drop_duplicates()

def get_rds_credentials():
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

    
def check_rds_table(table_name, rds_credentials):
    query = f"""
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = '{rds_credentials['RDS_DATABASE_NAME']}' AND table_name = '{table_name}';
    """
    
    logger.info(f"Prepared query to check table existence: {query}")

    try:
        # Connect to the RDS MySQL database using pymysql
        connection = pymysql.connect(
            host=rds_credentials['RDS_HOST'],
            user=rds_credentials['RDS_USER'],
            password=rds_credentials['RDS_PASSWORD'],
            database=rds_credentials['RDS_DATABASE_NAME'],
            port=int(rds_credentials['RDS_PORT'])
        )
        with connection.cursor() as cursor:
            # Execute the query
            logger.info(f"Executing query: {query}")
            cursor.execute(query)
            # Fetch the result
            result = cursor.fetchone()
            if result[0] > 0:
                logger.info(f"Table '{table_name}' exists in the RDS MySQL database.")
                return True  # Table exists
            else:
                logger.info(f"Table '{table_name}' does not exist in the RDS MySQL database.")
                return False  # Table doesn't exist
                
    except pymysql.MySQLError as e:
        logger.error(f"Error checking table in RDS MySQL: {str(e)}")
        raise
    
    finally:
        # Ensure that the connection is closed
        if 'connection' in locals() and connection:
            connection.close()
            logger.info("Database connection closed.") 



    
def format_value(value):
    if value is None:
        return 'NULL'
    elif isinstance(value, str):
        return "'{}'".format(value.replace("'", "''"))
    else:
        return str(value)


def upload_to_rds(df, table_name, rds_credentials):
    connection = pymysql.connect(
        host=rds_credentials['RDS_HOST'],
        user=rds_credentials['RDS_USER'],
        password=rds_credentials['RDS_PASSWORD'],
        database=rds_credentials['RDS_DATABASE_NAME'],
        port=rds_credentials['RDS_PORT']
    )

    columns = ", ".join(df.columns)
    
    for _, row in df.iterrows():
        values = ", ".join([format_value(val) for val in row])
        query = f"""
        INSERT INTO {table_name} ({columns}) 
        VALUES ({values})
        ON DUPLICATE KEY UPDATE {', '.join([f'{col}=VALUES({col})' for col in df.columns])};
        """
        
        print(f"Generated query: {query}")
        
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                connection.commit()
                print(f"Data loaded successfully into {table_name}")
                logger.info(f"Data loaded successfully into {table_name}")
        
        except Exception as e:
            print(f"Error inserting data: {e}")
            connection.rollback()
            raise
        
    connection.close()

def update_dynamodb(key, folder_name):
   
    table_name = DYNAMO_DB_TABLE_NAME

    partition_key_name = "FileName" 
    partition_key_value = "database_list" 

    try:
        # Derive basename from the key
        basename = key.split("/")[-1].replace(".csv", "")
        logger.info(f"Derived basename: {basename} from key: {key}")

        logger.info(f"Checking if partition key '{partition_key_value}' exists in table '{table_name}'")
        response = dynamodb_client.get_item(
            TableName=table_name,
            Key={partition_key_name: {'S': partition_key_value}}
        )

        if 'Item' not in response:
            logger.warning(f"Partition key '{partition_key_value}' does not exist. Creating it.")
            dynamodb_client.put_item(
                TableName=table_name,
                Item={
                    partition_key_name: {'S': partition_key_value},
                    partition_key_value: {'M': {}}
                }
            )
            logger.info(f"Created partition key '{partition_key_value}'.")
        logger.info(f"Fetching updated partition key '{partition_key_value}' from table '{table_name}'")
        response = dynamodb_client.get_item(
            TableName=table_name,
            Key={partition_key_name: {'S': partition_key_value}}
        )
        
        database_list_item = response.get('Item', {}).get(partition_key_value, {}).get('M', {})
        logger.info(f"Fetched database_list_item: {database_list_item}")

        if basename in database_list_item:
            logger.info(f"Basename '{basename}' exists. Updating its 'folder_name' to '{folder_name}'.")
            dynamodb_client.update_item(
                TableName=table_name,
                Key={partition_key_name: {'S': partition_key_value}},
                UpdateExpression=f"SET {partition_key_value}.#basename.folder_name = :folder_val",
                ExpressionAttributeNames={'#basename': basename},
                ExpressionAttributeValues={':folder_val': {'S': folder_name}}
            )
            logger.info(f"Successfully updated 'folder_name' for basename '{basename}'.")
        else:
            logger.warning(f"Basename '{basename}' does not exist. Creating it with 'folder_name' = '{folder_name}'.")
            dynamodb_client.update_item(
                TableName=table_name,
                Key={partition_key_name: {'S': partition_key_value}},
                UpdateExpression=f"SET {partition_key_value}.#basename = :new_basename",
                ExpressionAttributeNames={'#basename': basename},
                ExpressionAttributeValues={
                    ':new_basename': {'M': {'folder_name': {'S': folder_name}}}
                }
            )
            logger.info(f"Successfully created basename '{basename}' and added 'folder_name' '{folder_name}'.")
    except ClientError as e:
        logger.error(f"Error updating DynamoDB: {e}")
        raise
    
def send_sns_notification(table_name, file_path)
    message = (
        f"Data has been successfully loaded to the RDS table '{table_name}' "
        f"from the S3 file located at '{file_path}'.\n\n"
        "Details:\n"
        f"Table Name: {table_name}\n"
        f"S3 File Path: {file_path}"
    )
    subject = "Data Load Successful to RDS"

    try:
        response = boto3.client('sns').publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject=subject
        )
        logger.info(f"SNS Notification sent. Response: {response}")
    except ClientError as e:
        logger.error(f"Error sending SNS notification: {e}")
        raise
    
    
    

if __name__ == "__main__":
    main()
        
