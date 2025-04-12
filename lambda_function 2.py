import boto3
import json
import uuid

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')
stepfunctions = boto3.client('stepfunctions')

DYNAMO_TABLE_NAME = "DeltaLoadTracker"
SQS_QUEUE_URL = "https://sqs.us-west-1.amazonaws.com/982723143439/DatabaseFilePathQueue.fifo"
STATE_MACHINE_ARN = "arn:aws:states:us-west-1:982723143439:stateMachine:lambda-state-machine"

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
    except KeyError as e:
        error_message = f"Error: Missing expected field in event - {str(e)}"
        print(error_message)
        return {
            'statusCode': 400,
            'body': json.dumps({'error': error_message})
        }

    try:
        folder_name, file_name = object_key.split('/')
        base_name = file_name.rsplit('.', 1)[0]  # Extract 'student' from 'student.csv'
        timestamp = int(folder_name)  # Convert to int for comparison
    except Exception as e:
        error_message = f"Error parsing object key: {str(e)}"
        print(error_message)
        return {
            'statusCode': 400,
            'body': json.dumps({'error': error_message})
        }

    print(f"Bucket: {bucket_name}, Folder (Timestamp): {folder_name}, File: {file_name}, Base Name: {base_name}")
    
    table = dynamodb.Table(DYNAMO_TABLE_NAME)
    try:
        response = table.get_item(Key={'FileName': 'database_list'})
        print("DynamoDB Response:", response)

        if 'Item' not in response:
            print("No 'database_list' entry found. Proceeding with processing.")
            process_new_file(bucket_name, object_key, base_name, timestamp)
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'File processed successfully'})
            }

        database_list = response['Item'].get('database_list', {})
        if base_name not in database_list:
            print(f"No entry for {base_name} under 'database_list'. Proceeding with processing.")
            process_new_file(bucket_name, object_key, base_name, timestamp)
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'File processed successfully'})
            }
            
        folder_name = database_list[base_name].get('folder_name', None)
        if folder_name:
            existing_timestamp = int(folder_name.replace('_', ''))
            print(f"Existing timestamp for {base_name}: {existing_timestamp}")
        else:
            error_message = f"Folder name not found for {base_name}."
            print(error_message)
            return {
                'statusCode': 500,
                'body': json.dumps({'error': error_message})
            }
            
        if timestamp > existing_timestamp:
            print(f"New timestamp {timestamp} is greater. Proceeding with processing.")
            process_new_file(bucket_name, object_key, base_name, timestamp)
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'File processed successfully'})
            }
        else:
            print(f"Timestamp {timestamp} is not newer. No further action taken.")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No new action taken'})
            }

    except Exception as e:
        error_message = f"Error accessing DynamoDB: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }


def process_new_file(bucket_name, object_key, base_name, timestamp):
    file_path = f"s3://{bucket_name}/{object_key}"
    print(f"Sending file path to SQS: {file_path}")
    message_group_id = f"{base_name}-{timestamp}"
    
    sqs_message_body = {
    'Records': [
        {
            'file_path': file_path,  
            's3': {
                'bucket': {
                    'name': bucket_name
                },
                'object': {
                    'key': object_key
                }
            }
        }]
    }
    try:
        sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(sqs_message_body),
            MessageGroupId=message_group_id,
            MessageDeduplicationId=base_name + str(timestamp)
        )
        print(f"Message sent to SQS for {base_name}.")
        trigger_step_function(file_path)  # Trigger Step Function after processing
    except Exception as e:
        error_message = f"Error sending message to SQS: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }
    
def trigger_step_function(file_path):
    try:
        execution_name = f"Execution-{file_path.split('/')[-1]}-{str(uuid.uuid4())}"  # Adding UUID for uniqueness
        response = stepfunctions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=execution_name,  # Use the unique execution name
            input=json.dumps({"filePath": file_path})  # Pass the file path to Step Functions
        )
        print(f"Step Functions execution started. Execution ARN: {response['executionArn']}")
    except Exception as e:
        error_message = f"Error triggering Step Functions: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }
