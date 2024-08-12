import atexit
from apscheduler.schedulers.background import BackgroundScheduler

class Scheduler:
    def __init__(self, fetch_function, interval_minutes=1):
        self.fetch_function = fetch_function
        self.scheduler = BackgroundScheduler()
        self.interval_minutes = interval_minutes
    def start(self):
        self.scheduler.add_job(func=self.fetch_function, trigger="interval", minutes=1)
        self.scheduler.start()
        # Shut down the scheduler when exiting the app
        atexit.register(lambda: self.scheduler.shutdown())
