import httpx
from datetime import datetime
from f12scheduler.firebase_manager import FirestoreManager
from f12scheduler.logging_config import configure_logging

COLLECTION_NAME = "boca-data"
API_KEY = "e622b1469cdad8fb39da43fde1490356"
BASE_URL = "https://v3.football.api-sports.io"
TEAM_ID = 451
headers = {"x-apisports-key": API_KEY}
current_year = datetime.now().year


class FootballFetcher:
    def __init__(self):
        self.leaguesStandings = []
        self.fixture = []
        self.logger = configure_logging()

    async def start(self):
        await self.__getFixture()
        await self.__getStandings()
        await self.__storeData()

    async def __storeData(self):
        firestore_manager = FirestoreManager()
        collection_name = COLLECTION_NAME
        document_id = COLLECTION_NAME
        data = {"leaguesStandings": self.leaguesStandings, "fixture": self.fixture}

        firestore_manager.add_data(collection_name, document_id, data)

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
                self.logger.error(f"Failed to fetch fixture data: {e}")

    async def __getStandings(self):
        leagues = await self.__getLeagues()
        if leagues:
            for league in leagues:
                await self.__getLeagueStandings(league["league"]["id"], current_year)

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
                self.logger.error(f"Failed to fetch leagues data: {e}")
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
                    # Restructure the standings data
                    restructured_league = {
                        "id": leagueWithStandings["id"],
                        "name": leagueWithStandings["name"],
                        "country": leagueWithStandings["country"],
                        "logo": leagueWithStandings["logo"],
                        "flag": leagueWithStandings["flag"],
                        "season": leagueWithStandings["season"],
                        "standings": [],
                    }

                    # Flatten the nested standings array
                    for group in leagueWithStandings["standings"]:
                        for standing in group:
                            restructured_league["standings"].append(standing)

                    # Append the restructured data
                    self.leaguesStandings.append(restructured_league)

            except httpx.RequestError as e:
                self.logger.error(
                    f"Failed to fetch standings data for league {leagueId}: {e}"
                )
