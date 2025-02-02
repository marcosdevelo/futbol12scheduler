import atexit
from apscheduler.schedulers.background import (
    BackgroundScheduler,
)  # Use BackgroundScheduler instead
import asyncio

from logging_config import configure_logging


class Scheduler:
    def __init__(self, fetcher):
        self.scheduler = (
            BackgroundScheduler()
        )  # Switch to BackgroundScheduler for non-blocking
        self.fetcher = fetcher
        self.logger = configure_logging()

    def fetch(self):
        self.logger.info("Scheduler: Fetching data from the API")
        asyncio.run(self.fetcher.start())

    def start(self):
        # Register the shutdown callback before starting the scheduler
        atexit.register(lambda: self.scheduler.shutdown())

        # Schedule the job (e.g., run daily or hourly as needed)
        # self.scheduler.add_job(func=self.fetch, trigger="interval", days=1)

        # Schedule the job to run every day at 5 PM
        self.scheduler.add_job(func=self.fetch, trigger="cron", hour=17, minute=0)
        # Start the background scheduler
        self.scheduler.start()
