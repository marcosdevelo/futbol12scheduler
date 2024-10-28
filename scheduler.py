import atexit
from apscheduler.schedulers.blocking import BlockingScheduler
import asyncio


class Scheduler:
    def __init__(self, fetcher):
        self.scheduler = BlockingScheduler()
        self.fetcher = fetcher

    def fetch(self):
        asyncio.run(self.fetcher.start())

    def start(self):
        # Register the shutdown callback before starting the scheduler
        atexit.register(lambda: self.scheduler.shutdown())

        # Schedule the job
        self.scheduler.add_job(func=self.fetch, trigger="interval", minutes=1)

        # Start the blocking scheduler (this will keep the script running)
        self.scheduler.start()
