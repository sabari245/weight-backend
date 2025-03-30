# workers.py
import time
import logging
import json

# Import components needed by workers
from scale_reader import WeighingScale
import data_manager
import cloud_services

# Import config values
from config import (
    SERIAL_PORT, BAUDRATE, SERIAL_TIMEOUT,
    UPLOAD_BATCH_SIZE, UPLOAD_INTERVAL_SECONDS
)

def serial_reader_worker(stop_event):
    """ Worker thread function to read from serial port. """
    logging.info("Serial reader worker started.")
    scale = WeighingScale(port=SERIAL_PORT, baudrate=BAUDRATE, timeout=SERIAL_TIMEOUT)

    while not stop_event.is_set():
        if not scale.ser or not scale.ser.is_open:
            if not scale.connect():
                logging.warning(f"Failed to connect to {scale.port}, retrying in 5 seconds...")
                if stop_event.wait(5): break # Exit if stop requested during wait
                continue # Retry connection

        weight = scale.get_next_reading()
        if weight is not None:
            data_manager.add_reading(weight) # Add to cache, queue, log file
        else:
            # Avoid busy-waiting if scale sends data infrequently or there's a read error/timeout
            # The serial timeout handles blocking, but a small sleep here prevents tight loop on None
            time.sleep(0.05) # 50ms sleep

    scale.disconnect()
    logging.info("Serial reader worker stopped.")


def firehose_uploader_worker(stop_event):
    """ Worker thread function to upload data to Firehose. """
    logging.info("Firehose uploader worker started.")

    last_upload_time = time.time()
    records_batch_formatted = [] # Holds records formatted for Firehose API

    while not stop_event.is_set():
        batch_ready_to_upload = False
        current_time = time.time()

        # Drain the queue up to batch size or until empty
        try:
            while len(records_batch_formatted) < UPLOAD_BATCH_SIZE:
                # Use timeout to prevent blocking indefinitely if queue is empty
                reading = data_manager.upload_queue.get(block=True, timeout=0.1)
                # Format for Firehose: {'Data': b'json_string\n'}
                record = {'Data': (json.dumps(reading) + '\n').encode('utf-8')}
                records_batch_formatted.append(record)
                data_manager.upload_queue.task_done() # Mark task as done
        except data_manager.queue.Empty:
            # Queue is empty or timed out waiting
            pass

        # Check upload conditions
        batch_size = len(records_batch_formatted)
        time_since_last = current_time - last_upload_time

        if batch_size > 0 and \
           (batch_size >= UPLOAD_BATCH_SIZE or time_since_last >= UPLOAD_INTERVAL_SECONDS):
            batch_ready_to_upload = True

        if batch_ready_to_upload:
            success = cloud_services.upload_batch_to_firehose(records_batch_formatted)
            if success:
                records_batch_formatted = [] # Clear batch on success
                last_upload_time = current_time
            else:
                # Upload failed (error already logged by cloud_services)
                # Keep records in records_batch_formatted to retry next time.
                # Consider adding a maximum retry limit or dead-letter handling
                logging.warning(f"Upload failed. {len(records_batch_formatted)} records will be retried.")
                # Update last_upload_time even on failure to prevent rapid retries in busy loops
                last_upload_time = current_time
                # Optional: Add a delay after failure before next attempt
                if stop_event.wait(2): break # Wait briefly, check for stop

        # Prevent busy-waiting if no upload happened and queue was empty
        if not batch_ready_to_upload:
             if stop_event.wait(0.5): break # Wait 0.5s, check for stop

    # --- Cleanup on Stop ---
    logging.info("Firehose uploader thread stopping.")
    # Try one last upload if there are pending records
    if records_batch_formatted:
        logging.info(f"Attempting final upload of {len(records_batch_formatted)} remaining records...")
        cloud_services.upload_batch_to_firehose(records_batch_formatted)

    logging.info("Firehose uploader worker stopped.")