from flask import Flask, jsonify

from f12scheduler.fetcher import FootballFetcher
from scheduler import Scheduler

app = Flask(__name__)

fetcher = FootballFetcher()
appScheduler = Scheduler(fetcher)
appScheduler.start()


@app.route("/")
def index():
    return jsonify({"message": "API Scheduler is running!"})


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
