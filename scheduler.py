import atexit
from apscheduler.schedulers.background import BackgroundScheduler


class Scheduler:
    def __init__(self, fetcher):
        self.scheduler = BackgroundScheduler()
        self.fetcher = fetcher

    def fetch_function(self):
        self.fetcher.start()

    def start(self):
        print("Starting Scheduler")
        self.scheduler.add_job(func=self.fetch_function, trigger="interval", minutes=1)
        self.scheduler.start()
        # Shut down the scheduler when exiting the app
        atexit.register(lambda: self.scheduler.shutdown())
