from io import StringIO
import pandas as pd
import boto3
import json
import os
import logging

log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level)
CHUNK_SIZE = os.getenv('CHUNK_SIZE', 100)
region_name = os.getenv('AWS_REGION', 'us-east-1')
sqs = boto3.client('sqs', region_name=region_name)
queue_name = 'test'
queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']

# Generator function to read CSV file in chunks
def read_csv_in_chunks(file_path, chunk_size=1000):
    try:
        # Use chunksize and yield to return chunks lazily
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            yield chunk
    except Exception as e:
        logging.error(f"Error reading CSV in chunks: {e}")

# Function to send a chunk to SQS
def send_chunk_to_sqs(queue_url, chunk):
    csv_buffer = StringIO()
    chunk.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=csv_data
    )
    logging.debug(f"Message sent: {response.get('MessageId')}")

# Main script to read CSV, chunk it, and send to SQS
def process_csv_and_send_to_sqs(file_path, chunk_size=1000):
    # Lazily read the CSV_ in chunks
    for chunk in read_csv_in_chunks(file_path, chunk_size):
        send_chunk_to_sqs(queue_url, chunk)
        logging.debug(f"Chunk sent successfully with {len(chunk)} rows.")

if __name__ == "__main__":
    # Define your CSV file path
    csv_file_path = "data.csv"  # Replace with the path to your CSV file
    process_csv_and_send_to_sqs(csv_file_path, chunk_size=CHUNK_SIZE)  # Adjust chunk_size if necessary