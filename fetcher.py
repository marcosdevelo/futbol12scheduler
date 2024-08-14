import httpx
import asyncio
from datetime import datetime
import logging

API_KEY = "e622b1469cdad8fb39da43fde1490356"
BASE_URL = "https://v3.football.api-sports.io"
TEAM_ID = 451
headers = {"x-apisports-key": API_KEY}
current_year = datetime.now().year
# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class FootballFetcher:
    def __init__(self):
        self.leaguesStandings = []
        self.fixture = []

    async def start(self):
        await self.__getFixture()
        await self.__getStandings()
        logger.info("Final Fetched data")
        logger.info(f"Leagues Standings: {self.leaguesStandings}")
        logger.info(f"Fixture: {self.fixture}")

    async def __getFixture(self):
        url = f"{BASE_URL}/fixtures"
        body = {"team": TEAM_ID, "next": 6}
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=headers, params=body)
                response.raise_for_status()
                data = response.json()
                self.fixture = data["response"]
            except httpx.RequestError as e:
                logger.error(f"Failed to fetch fixture data: {e}")

    async def __getStandings(self):
        leagues = await self.__getLeagues()
        if leagues:
            tasks = [
                self.__getLeagueStandings(league["league"]["id"], current_year)
                for league in leagues
            ]
            await asyncio.gather(*tasks)

    async def __getLeagues(self):
        body = {"team": TEAM_ID}
        url = f"{BASE_URL}/leagues"
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=headers, params=body)
                response.raise_for_status()
                data = response.json()
                leagues = data["response"]
                # Filter leagues to include only those with year 2024 and standings coverage
                filtered_leagues = []
                for league in leagues:
                    for season in league["seasons"]:
                        if (
                            season["year"] == current_year
                            and season["coverage"]["standings"]
                        ):
                            filtered_leagues.append(league)
                            break  # Stop checking other seasons once a match is found
                return filtered_leagues
            except httpx.RequestError as e:
                logger.error(f"Failed to fetch leagues data: {e}")
                return None

    async def __getLeagueStandings(self, leagueId, season):
        body = {"league": leagueId, "season": season}
        url = f"{BASE_URL}/standings"
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=headers, params=body)
                response.raise_for_status()
                data = response.json()
                if len(data["response"]) > 0:
                    leagueWithStandings = data["response"][0]["league"]
                    self.leaguesStandings.append(leagueWithStandings)
            except httpx.RequestError as e:
                logger.error(
                    f"Failed to fetch standings data for league {leagueId}: {e}"
                )
