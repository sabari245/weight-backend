# config.py
import os
import logging
from dotenv import load_dotenv

load_dotenv()

# --- Serial Port Configuration ---
SERIAL_PORT = os.getenv('SERIAL_PORT', 'COM4')
BAUDRATE = int(os.getenv('BAUDRATE', 9600))
SERIAL_TIMEOUT = int(os.getenv('SERIAL_TIMEOUT', 1)) # Added timeout config

# --- AWS Configuration ---
FIREHOSE_STREAM_NAME = os.getenv('FIREHOSE_STREAM_NAME')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
AWS_REGION = os.getenv('AWS_REGION')

# --- Data Management Configuration ---
MAX_CACHE_SIZE = int(os.getenv('MAX_CACHE_SIZE', 200))
UPLOAD_BATCH_SIZE = int(os.getenv('UPLOAD_BATCH_SIZE', 500))
UPLOAD_INTERVAL_SECONDS = int(os.getenv('UPLOAD_INTERVAL_SECONDS', 60))
LOG_FILE_NAME = os.getenv('LOG_FILE_NAME', 'weighing_logs.jsonl')

# --- API Configuration ---
API_HOST = os.getenv('API_HOST', '127.0.0.1')
API_PORT = int(os.getenv('API_PORT', 5000))

# --- Validation ---
def validate_config():
    """ Checks if essential cloud configurations are set. """
    valid = True
    if not FIREHOSE_STREAM_NAME:
        logging.error("Missing required environment variable: FIREHOSE_STREAM_NAME")
        valid = False
    if not S3_BUCKET_NAME:
        logging.error("Missing required environment variable: S3_BUCKET_NAME")
        valid = False
    if not AWS_REGION:
        logging.error("Missing required environment variable: AWS_REGION")
        valid = False
    return valid