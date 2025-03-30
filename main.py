# main.py
import threading
import time
import logging
import signal
import sys
import config
from workers import serial_reader_worker, firehose_uploader_worker
from api import app

stop_event = threading.Event()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

def signal_handler(sig, frame):
    logging.info(f"Received signal {sig}. Initiating graceful shutdown...")
    stop_event.set() # Signal threads to stop

def main():
    logging.info("Starting Weighing Scale Service...")

    if not config.validate_config():
        logging.critical("Essential configuration missing. Exiting.")
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    threads = []
    serial_thread = threading.Thread(target=serial_reader_worker, args=(stop_event,), name="SerialReader")
    uploader_thread = threading.Thread(target=firehose_uploader_worker, args=(stop_event,), name="FirehoseUploader")

    threads.extend([serial_thread, uploader_thread])

    serial_thread.start()
    uploader_thread.start()

    # Start Flask API server
    logging.info(f"Starting Flask API server on http://{config.API_HOST}:{config.API_PORT}")
    try:
        # Option 1: Run Flask in its own thread (simple for this case)
        api_thread = threading.Thread(target=lambda: app.run(host=config.API_HOST, port=config.API_PORT, threaded=True), name="APIServer", daemon=True)
        api_thread.start()

        while not stop_event.is_set():
             all_alive = all(t.is_alive() for t in threads)
             if not all_alive:
                 logging.warning("One or more worker threads have unexpectedly stopped.")
                 stop_event.set() 
                 break
             time.sleep(1) 

        # Option 2 (Alternative): Run app.run directly, shutdown happens in signal_handler/finally
        # app.run(host=config.API_HOST, port=config.API_PORT, threaded=True)

    except Exception as e:
         logging.exception(f"API Server failed: {e}")
         stop_event.set() 

    finally:
        logging.info("Main thread received stop signal or detected thread exit.")
        if not stop_event.is_set():
             stop_event.set() 

        logging.info("Waiting for worker threads to finish...")
        for t in threads:
            try:

                if t.name == "FirehoseUploader":
                    t.join(timeout=config.UPLOAD_INTERVAL_SECONDS + 10) # Give uploader more time
                else:
                    t.join(timeout=10) # General timeout
                if t.is_alive():
                    logging.warning(f"Thread {t.name} did not finish within timeout.")
            except Exception as e:
                 logging.exception(f"Error joining thread {t.name}: {e}")

        logging.info("Shutdown complete.")
        sys.exit(0) # Ensure clean exit


if __name__ == "__main__":
    main()