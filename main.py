# main.py
import threading
import time
import logging
import signal
import sys

# Import configurations and validation
import config

# Import worker functions
from workers import serial_reader_worker, firehose_uploader_worker

# Import the Flask app instance
from api import app

# --- Global Stop Event ---
stop_event = threading.Event()

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

# --- Signal Handler for Graceful Shutdown ---
def signal_handler(sig, frame):
    logging.info(f"Received signal {sig}. Initiating graceful shutdown...")
    stop_event.set() # Signal threads to stop

# --- Main Execution ---
def main():
    logging.info("Starting Weighing Scale Service...")

    # Validate configuration first
    if not config.validate_config():
        logging.critical("Essential configuration missing. Exiting.")
        sys.exit(1)

    # Register signal handlers for SIGINT (Ctrl+C) and SIGTERM
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create and start worker threads
    threads = []
    serial_thread = threading.Thread(target=serial_reader_worker, args=(stop_event,), name="SerialReader")
    uploader_thread = threading.Thread(target=firehose_uploader_worker, args=(stop_event,), name="FirehoseUploader")

    threads.extend([serial_thread, uploader_thread])

    serial_thread.start()
    uploader_thread.start()

    # Start Flask API server
    # Use a production server like waitress or gunicorn instead of app.run in production
    logging.info(f"Starting Flask API server on http://{config.API_HOST}:{config.API_PORT}")
    try:
        # Run Flask in a way that doesn't block the main thread indefinitely
        # if we want the main thread to handle shutdown waits.
        # Option 1: Run Flask in its own thread (simple for this case)
        api_thread = threading.Thread(target=lambda: app.run(host=config.API_HOST, port=config.API_PORT, threaded=True), name="APIServer", daemon=True)
        api_thread.start()
        # Keep main thread alive to wait for stop signal or thread completion
        while not stop_event.is_set():
             # Check if worker threads are still alive (optional)
             all_alive = all(t.is_alive() for t in threads)
             if not all_alive:
                 logging.warning("One or more worker threads have unexpectedly stopped.")
                 stop_event.set() # Trigger shutdown if a worker dies
                 break
             time.sleep(1) # Main thread sleeps, waiting for stop signal

        # Option 2 (Alternative): Run app.run directly, shutdown happens in signal_handler/finally
        # app.run(host=config.API_HOST, port=config.API_PORT, threaded=True)
        # This might make joining threads trickier if app.run blocks fully until Ctrl+C

    except Exception as e:
         logging.exception(f"API Server failed: {e}")
         stop_event.set() # Trigger shutdown on API server error

    finally:
        logging.info("Main thread received stop signal or detected thread exit.")
        if not stop_event.is_set():
             stop_event.set() # Ensure stop is set if exiting loop for other reasons

        logging.info("Waiting for worker threads to finish...")
        for t in threads:
            try:
                # Add appropriate timeouts for joining
                if t.name == "FirehoseUploader":
                    t.join(timeout=config.UPLOAD_INTERVAL_SECONDS + 10) # Give uploader more time
                else:
                    t.join(timeout=10) # General timeout
                if t.is_alive():
                    logging.warning(f"Thread {t.name} did not finish within timeout.")
            except Exception as e:
                 logging.exception(f"Error joining thread {t.name}: {e}")

        # Note: If API server was run as daemon thread, it will exit automatically.
        # If run directly with app.run, it might need separate handling if not stopped by signal.

        logging.info("Shutdown complete.")
        sys.exit(0) # Ensure clean exit


if __name__ == "__main__":
    main()