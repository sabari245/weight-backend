# data_manager.py
import queue
import threading
import json
import logging
from collections import deque
from datetime import datetime, timezone
import uuid

# Import specific config values needed
from config import MAX_CACHE_SIZE, LOG_FILE_NAME

# --- Shared Data Structures ---
# Thread-safe queue for readings pending upload
upload_queue = queue.Queue()
# Thread-safe deque for the latest readings cache
latest_readings_cache = deque(maxlen=MAX_CACHE_SIZE)
# Lock for accessing the local log file
log_file_lock = threading.Lock()

def add_reading(weight):
    """Creates a reading structure, adds to cache, queue, and logs locally."""
    if weight is None:
        return None # Don't process None values

    timestamp = datetime.now(timezone.utc).isoformat()
    reading_id = str(uuid.uuid4())
    reading_data = {
        "uuid": reading_id,
        "timestamp": timestamp,
        "weight": weight
    }
    logging.debug(f"Processed reading: {reading_data}")

    # Add to cache (deque is thread-safe for append/pop)
    latest_readings_cache.append(reading_data)

    # Add to upload queue (Queue is thread-safe)
    upload_queue.put(reading_data)

    # Append to local log file (needs external lock)
    try:
        with log_file_lock:
            with open(LOG_FILE_NAME, 'a') as f:
                json.dump(reading_data, f)
                f.write('\n')
    except IOError as e:
        logging.error(f"Error writing to log file {LOG_FILE_NAME}: {e}")
    except Exception as e:
        logging.exception(f"Unexpected error writing to log file: {e}")

    return reading_data # Return the processed data if needed elsewhere


def get_latest_readings(since_uuid=None):
    """Retrieves readings from the cache, optionally filtering by since_uuid."""
    # Get a snapshot (list copy) for safe iteration
    readings_snapshot = list(latest_readings_cache)

    if not since_uuid:
        return readings_snapshot # Return all if no filter

    try:
        index = -1
        # Iterate backwards through the snapshot
        for i in range(len(readings_snapshot) - 1, -1, -1):
            if readings_snapshot[i].get('uuid') == since_uuid:
                index = i
                break
        if index != -1:
            # Return only readings *after* the found index
            return readings_snapshot[index + 1:]
        else:
            # UUID not found (maybe old/expired from cache), return the full snapshot
            logging.debug(f"since_uuid {since_uuid} not found in cache, returning all {len(readings_snapshot)} cached items.")
            return readings_snapshot
    except Exception as e:
         logging.exception(f"Error processing since_uuid '{since_uuid}': {e}")
         return readings_snapshot # Fallback to returning all on error