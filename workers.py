# workers.py
import time
import logging
import json

from scale_reader import WeighingScale
import data_manager
import cloud_services

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
                if stop_event.wait(5): break 
                continue 

        weight = scale.get_next_reading()
        if weight is not None:
            data_manager.add_reading(weight) 
        else:

            time.sleep(0.05) # 50ms sleep

    scale.disconnect()
    logging.info("Serial reader worker stopped.")


def firehose_uploader_worker(stop_event):
    """ Worker thread function to upload data to Firehose. """
    logging.info("Firehose uploader worker started.")

    last_upload_time = time.time()
    records_batch_formatted = [] 

    while not stop_event.is_set():
        batch_ready_to_upload = False
        current_time = time.time()


        try:
            while len(records_batch_formatted) < UPLOAD_BATCH_SIZE:

                reading = data_manager.upload_queue.get(block=True, timeout=0.1)
                # Format for Firehose: {'Data': b'json_string\n'}
                record = {'Data': (json.dumps(reading) + '\n').encode('utf-8')}
                records_batch_formatted.append(record)
                data_manager.upload_queue.task_done()
        except data_manager.queue.Empty:
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
                logging.warning(f"Upload failed. {len(records_batch_formatted)} records will be retried.")
                last_upload_time = current_time
                if stop_event.wait(2): break 

        if not batch_ready_to_upload:
             if stop_event.wait(0.5): break 

    logging.info("Firehose uploader thread stopping.")

    if records_batch_formatted:
        logging.info(f"Attempting final upload of {len(records_batch_formatted)} remaining records...")
        cloud_services.upload_batch_to_firehose(records_batch_formatted)

    logging.info("Firehose uploader worker stopped.")