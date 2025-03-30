# cloud_services.py
import boto3
from botocore.exceptions import ClientError
import logging
import json
from datetime import datetime

# Import necessary config values
from config import AWS_REGION, S3_BUCKET_NAME, FIREHOSE_STREAM_NAME

# Initialize clients globally or within functions as needed
# Global initialization can save time if functions are called frequently
# Ensure thread safety if clients are shared and methods aren't thread-safe (boto3 clients generally are)
try:
    if AWS_REGION:
        firehose_client = boto3.client('firehose', region_name=AWS_REGION)
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        logging.info(f"Initialized boto3 clients for Firehose and S3 in region {AWS_REGION}")
    else:
        firehose_client = None
        s3_client = None
        logging.warning("AWS_REGION not set. AWS clients not initialized.")
except Exception as e:
    logging.exception(f"Failed to initialize Boto3 clients: {e}")
    firehose_client = None
    s3_client = None


def upload_batch_to_firehose(records_batch):
    """
    Uploads a batch of records to Kinesis Firehose.
    Args:
        records_batch (list): A list of dicts, where each dict is {'Data': b'json_string\n'}.
    Returns:
        bool: True if upload likely succeeded, False otherwise.
    """
    if not firehose_client:
        logging.error("Firehose client not initialized. Cannot upload.")
        return False
    if not FIREHOSE_STREAM_NAME:
         logging.error("FIREHOSE_STREAM_NAME not configured. Cannot upload.")
         return False
    if not records_batch:
        logging.debug("No records in batch to upload.")
        return True # Nothing to upload is considered success

    batch_size = len(records_batch)
    logging.info(f"Attempting to upload batch of {batch_size} records to Firehose stream: {FIREHOSE_STREAM_NAME}")

    try:
        # Firehose PutRecordBatch limit is 500 records / 4MB
        if batch_size > 500:
            logging.warning(f"Batch size ({batch_size}) exceeds Firehose limit (500). Uploading only first 500.")
            records_batch = records_batch[:500] # Simple truncation, could split instead

        response = firehose_client.put_record_batch(
            DeliveryStreamName=FIREHOSE_STREAM_NAME,
            Records=records_batch
        )
        logging.debug(f"Firehose response: {response}")
        failed_count = response.get('FailedPutCount', 0)
        if failed_count > 0:
            logging.error(f"Firehose put_record_batch failed for {failed_count} records.")
            # Implement more detailed error inspection from response['RequestResponses'] if needed
            return False
        else:
            logging.info(f"Successfully uploaded {len(records_batch)} records to {FIREHOSE_STREAM_NAME}.")
            return True
    except ClientError as e:
        logging.error(f"AWS ClientError uploading to Firehose: {e}")
        return False
    except Exception as e:
        logging.exception(f"Unexpected error uploading to Firehose: {e}")
        return False


def fetch_historical_data_from_s3(date_str):
    """ Fetches and parses log data for a given date from S3. """
    if not s3_client:
        logging.error("S3 client not initialized. Cannot fetch historical data.")
        return None
    if not S3_BUCKET_NAME:
        logging.error("S3_BUCKET_NAME not configured. Cannot fetch historical data.")
        return None

    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d')
        # Adjust prefix based on your Firehose S3 partitioning scheme
        s3_prefix = f"data/{target_date.strftime('%Y/%m/%d')}/"
        logging.info(f"Fetching historical data from s3://{S3_BUCKET_NAME}/{s3_prefix}")

        all_readings = []
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=s3_prefix)

        object_count = 0
        for page in pages:
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                object_count += 1
                s3_key = obj['Key']
                logging.debug(f"Processing S3 object: {s3_key}")
                try:
                    response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
                    # Handle potential gzip compression if Firehose is configured for it
                    # if s3_key.endswith('.gz'):
                    #    import gzip
                    #    body = gzip.decompress(response['Body'].read())
                    # else:
                    body = response['Body'].read()

                    content = body.decode('utf-8')
                    lines = content.strip().split('\n')
                    for line in lines:
                        if line:
                            try:
                                reading = json.loads(line)
                                all_readings.append(reading)
                            except json.JSONDecodeError:
                                logging.warning(f"Skipping invalid JSON line in {s3_key}: {line[:100]}...")
                except ClientError as e:
                    logging.error(f"Error fetching/reading S3 object {s3_key}: {e}") # Log S3 key
                except Exception as e:
                    logging.exception(f"Unexpected error processing S3 object {s3_key}: {e}") # Log S3 key

        logging.info(f"Processed {object_count} S3 objects, fetched {len(all_readings)} historical records for {date_str}.")
        # Sort by timestamp for chronological order
        all_readings.sort(key=lambda x: x.get('timestamp', ''))
        return all_readings

    except ValueError:
        logging.error(f"Invalid date format provided: {date_str}. Use YYYY-MM-DD.")
        return None # Indicate format error
    except ClientError as e:
        logging.error(f"AWS ClientError accessing S3 bucket {S3_BUCKET_NAME}: {e}")
        return None # Indicate AWS error
    except Exception as e:
        logging.exception(f"Unexpected error fetching historical data for {date_str}: {e}")
        return None # Indicate general error