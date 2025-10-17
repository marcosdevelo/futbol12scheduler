import threading
import os
import asyncio

from flask import Flask, jsonify, request
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


# API endpoint to fetch top scorers for multiple leagues
@app.route("/get-top-scorers", methods=["POST"])
async def get_top_scorers():
    logger.info("Top scorers endpoint called.")
    
    try:
        # Get JSON data from request
        data = request.get_json()
        
        # Validate input
        if not data or "league_ids" not in data:
            return jsonify({
                "status": 400,
                "message": "Missing required field 'league_ids' in request body"
            }), 400
        
        league_ids = data["league_ids"]
        
        # Validate league_ids is a list
        if not isinstance(league_ids, list) or len(league_ids) == 0:
            return jsonify({
                "status": 400,
                "message": "'league_ids' must be a non-empty array"
            }), 400
        
        # Validate all items in league_ids are integers
        if not all(isinstance(lid, int) for lid in league_ids):
            return jsonify({
                "status": 400,
                "message": "All league_ids must be integers"
            }), 400
        
        logger.info(f"Fetching top scorers for leagues: {league_ids}")
        
        # Create a new fetcher instance for this request to avoid state conflicts
        temp_fetcher = FootballFetcher()
        result = await temp_fetcher.fetch_top_scorers_for_leagues(league_ids)
        
        # Return the result
        if result["status"] == "success":
            return jsonify({
                "status": 200,
                "message": result["message"],
                "data": result["data"],
                "errors": result["errors"]
            })
        else:
            return jsonify({
                "status": 500,
                "message": result["message"],
                "errors": result["errors"]
            }), 500
            
    except Exception as e:
        error_msg = f"Error in get_top_scorers endpoint: {str(e)}"
        logger.error(error_msg)
        return jsonify({
            "status": 500,
            "message": "Internal server error",
            "error": error_msg
        }), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    serve(app, host="0.0.0.0", port=port)
