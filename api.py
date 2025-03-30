# api.py
from flask import Flask, jsonify, request
from flask_cors import CORS
import logging
from datetime import datetime, timezone

# Import functions from other modules
import data_manager
import cloud_services
# Import config if needed for constants like API host/port (though usually passed in main)
# from config import API_HOST, API_PORT

app = Flask(__name__)
CORS(app) # Allow cross-origin requests

@app.route('/latest', methods=['GET'])
def get_latest():
    """ Returns latest readings, optionally filtering by 'since_uuid'. """
    since_uuid = request.args.get('since_uuid')
    latest_data = data_manager.get_latest_readings(since_uuid)
    return jsonify(latest_data)

@app.route('/historical', methods=['GET'])
def get_historical():
    """ Fetches data for a specific date from S3. """
    date_str = request.args.get('date') # Expects 'YYYY-MM-DD'
    if not date_str:
        return jsonify({"error": "Missing 'date' query parameter (YYYY-MM-DD)"}), 400

    try:
        historical_data = cloud_services.fetch_historical_data_from_s3(date_str)

        if historical_data is not None:
            return jsonify(historical_data)
        else:

            return jsonify({"error": f"Failed to retrieve data for {date_str}. Check service logs."}), 500
    except Exception as e:
        logging.exception(f"Error in /historical endpoint handler: {e}")
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """ Basic health check endpoint. """
    return jsonify({"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}), 200