from io import StringIO
import boto3
import pandas as pd
import time
import os
import logging

region_name = os.getenv('AWS_REGION', 'us-east-1')
workload_name = os.getenv('WORKLOAD_NAME', time.strftime("%Y%m%d"))
serializer = os.getenv('SERIALIZER', "csv") 

sqs = boto3.client('sqs', region_name=region_name)
s3 = boto3.client('s3', region_name=region_name)
ecs = boto3.client('ecs', region_name=region_name)
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level)


# SQS and S3 configurations
queue_name = 'test'
bucket_name = 'esimja-test'

queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']

# Function to process a single message (chunk of DataFrame)
def process_message(message):
    try:
        body = message['Body']
        # Convert the CSV string in the body into a DataFrame
        csv_buffer = StringIO(body)
        chunk_df = pd.read_csv(csv_buffer) 
        # Process the DataFrame and get stats
        processed_df, stats = process_dataframe_function(chunk_df)
        
        # Save the processed DataFrame and stats to S3
        output_key = f'processed/{message["MessageId"]}.csv'
        stats_key = f'stats/{message["MessageId"]}_stats.json'
        
        # Save the processed DataFrame
        processed_df.to_csv(f'/tmp/{message["MessageId"]}_processed.csv', index=False)
        s3.upload_file(f'/tmp/{message["MessageId"]}_processed.csv', bucket_name, output_key)

        # Save the stats
        with open(f'/tmp/{message["MessageId"]}_stats.json', 'w') as f:
            f.write(str(stats))
            s3.upload_file(f'/tmp/{message["MessageId"]}_stats.json', bucket_name, stats_key)
            # Acknowledge the message
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
    except Exception as e:
        logging.error(e)
        release_message(message=message)

def release_message(message):
    sqs.change_message_visibility(
    QueueUrl=queue_url,
    ReceiptHandle=message['ReceiptHandle'],
    VisibilityTimeout=0 
    )

def process_dataframe_function(df):
    if (df['a'].eq("asdasdasdasdText 701").any()):
        raise ValueError("aa")
    df['f'] = df['b'].add(1)
    return df, {"test":[1,1]}

# Function to check if the queue is empty or only has invisible messages
def is_queue_empty():
    response = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
    )
    logging.debug(f"Queue Attributes = {response}")
    messages_available = int(response['Attributes']['ApproximateNumberOfMessages'])
    invisible_messages = int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])

    return messages_available == 0 and invisible_messages == 0

# Main loop to continuously process messages
def main_loop():
    while True:
        # Poll the queue for messages
        response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=5)
        if 'Messages' in response:
            process_message(response['Messages'][0])
        else:
            # If no messages left and no invisible messages, break the loop
            if is_queue_empty():
                logging.info("No messages in queue.")
                break

if __name__ == '__main__':
    main_loop()
