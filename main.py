import threading
import os
import asyncio

from flask import Flask, jsonify
from flask_cors import CORS
from fetcher import FootballFetcher
from logging_config import configure_logging
# from scheduler import Scheduler
from waitress import serve

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

fetcher = FootballFetcher()
# appScheduler = Scheduler(fetcher)

logger = configure_logging()


# # Function to start the scheduler in a background thread
# def start_scheduler():
#     appScheduler.start()
#
#
# # Run the scheduler in a separate thread when the app starts
# threading.Thread(target=start_scheduler, daemon=True).start()


@app.route("/", methods=["GET"])
def home_welcome():
    logger.info("home page API.")
    return jsonify({"status": 200, "message": "Welcome to the home page."})


@app.route("/ping", methods=["GET"])
def ping():
    logger.info("Pinged API.")
    return jsonify({"status": 200, "message": "pong"})


# API endpoint to manually trigger the fetcher
@app.route("/trigger-fetcher", methods=["GET"])
async def trigger_fetcher():
    logger.info("Manually triggering fetcher.")
    result = await fetcher.start()  # Await the async fetcher
    
    # Return the actual result from the fetcher
    if result["status"] == "success":
        return jsonify({
            "status": 200,
            "message": result["message"],
            "errors": result["errors"],
            "data_summary": result["data_summary"]
        })
    else:
        return jsonify({
            "status": 500,
            "message": result["message"],
            "errors": result["errors"]
        })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    serve(app, host="0.0.0.0", port=port)
