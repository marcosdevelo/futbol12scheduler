from sched import scheduler

from flask import Flask, jsonify


from fetcher import FootballFetcher
from scheduler import Scheduler

app = Flask(__name__)

fetcher = FootballFetcher()
appScheduler = Scheduler(fetch_function=fetcher.startFetchRoutine(),interval_minutes=1)
appScheduler.start()

@app.route('/')
def index():
    return jsonify({"message": "API Scheduler is running!"})

if __name__ == '__main__':
    app.run(debug=True)
