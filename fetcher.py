import requests
from datetime import datetime
import logging

API_KEY = "e622b1469cdad8fb39da43fde1490356"
BASE_URL = "https://v3.football.api-sports.io"
TEAM_ID = 451
headers = {"x-apisports-key": API_KEY}

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class FootballFetcher:
    def __init__(self):
        self.leaguesStandings = []
        self.fixture = []

    def start(self):
        self.__getFixture()
        self.__getStandings()
        logger.info("Final Fetched data")
        logger.info(f"Leagues Standings: {self.leaguesStandings}")
        logger.info(f"Fixture: {self.fixture}")

    def __getFixture(self):
        url = f"{BASE_URL}/fixtures"
        body = {"team": TEAM_ID, "next": 6}
        try:
            response = requests.get(url, headers=headers, params=body)
            response.raise_for_status()
            data = response.json()
            self.fixture = data["response"]
        except requests.RequestException as e:
            logger.error(f"Failed to fetch fixture data: {e}")

    def __getStandings(self):
        current_year = datetime.now().year
        leagues = self.__getLeagues()
        if leagues:
            for league in leagues:
                self.__getLeagueStandings(league["league"]["id"], current_year)

    def __getLeagues(self):
        body = {
            "team": TEAM_ID,
        }
        url = f"{BASE_URL}/leagues"
        try:
            response = requests.get(url, headers=headers, params=body)
            response.raise_for_status()
            data = response.json()
            return data["response"]
        except requests.RequestException as e:
            logger.error(f"Failed to fetch leagues data: {e}")
            return None

    def __getLeagueStandings(self, leagueId, season):
        body = {"league": leagueId, "season": season}
        url = f"{BASE_URL}/standings"
        try:
            response = requests.get(url, headers=headers, params=body)
            response.raise_for_status()
            data = response.json()
            if len(data["response"]) > 0 and "league" in data["response"][0]:
                leagueWithStandings = data["response"][0]["league"]
                self.leaguesStandings.append(leagueWithStandings)
        except requests.RequestException as e:
            logger.error(f"Failed to fetch standings data for league {leagueId}: {e}")
