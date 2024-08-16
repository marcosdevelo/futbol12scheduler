import asyncio

from f12scheduler.fetcher import FootballFetcher
from scheduler import Scheduler


def main():
    fetcher = FootballFetcher()
    asyncio.run(fetcher.start())
    # appScheduler = Scheduler(fetcher)
    # appScheduler.start()


if __name__ == "__main__":
    main()
