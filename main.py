import threading
import os

from flask import Flask, jsonify
from flask_cors import CORS
from fetcher import FootballFetcher
from logging_config import configure_logging
from scheduler import Scheduler
from waitress import serve

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

fetcher = FootballFetcher()
appScheduler = Scheduler(fetcher)

logger = configure_logging()


# Function to start the scheduler in a background thread
def start_scheduler():
    appScheduler.start()


# Run the scheduler in a separate thread when the app starts
threading.Thread(target=start_scheduler, daemon=True).start()


@app.route("/", methods=["GET"])
async def home_welcome():
    logger.info("home page API.")
    return jsonify({"status": 200, "message": "Welcome to the home page."})


# API endpoint to manually trigger the fetcher
@app.route("/trigger-fetcher", methods=["GET"])
async def trigger_fetcher():
    logger.info("Manually triggering fetcher.")
    await fetcher.start()  # Directly call the fetcher's start method
    return jsonify({"status": 200, "message": "Fetcher triggered."})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    serve(app, host="0.0.0.0", port=port)
