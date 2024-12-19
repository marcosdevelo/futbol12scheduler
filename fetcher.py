import httpx
from datetime import datetime, timedelta

import pytz

from f12scheduler.firebase_manager import FirestoreManager
from f12scheduler.k import K
from f12scheduler.logging_config import configure_logging


current_year = datetime.now().year


class FootballFetcher:
    def __init__(self):
        self.leaguesStandings = []
        self.fixture = []
        self.lastGame = []
        self.logger = configure_logging()
        self.firestore_manager = FirestoreManager()
        self.processed_league_ids = set()

    async def start(self):
        await self.resetData()
        await self.__getFixture()
        await self.__getStandings()
        await self.__getLastGameResults()
        await self.__storeData()

    async def resetData(self):
        self.leaguesStandings = []
        self.fixture = []
        self.lastGame = []

    async def __storeData(self):
        collection_name = K.COLLECTION_NAME
        document_id = K.COLLECTION_NAME
        newData = {}

        if len(self.leaguesStandings) > 0:
            newData["leaguesStandings"] = self.leaguesStandings
        if len(self.fixture) > 0:
            newData["fixture"] = self.fixture
        if len(self.lastGame) > 0:
            newData["lastGame"] = self.lastGame
        self.firestore_manager.update_data(collection_name, document_id, newData)

        await self.__check_for_tomorrow_games()

    async def __getLastGameResults(self):
        url = f"{K.BASE_URL}/fixtures"
        body = {"team": K.TEAM_ID, "last": 1}
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=K.headers, params=body)
                response.raise_for_status()
                data = response.json()
                self.lastGame = data["response"]
            except httpx.RequestError as e:
                self.logger.error(f"Failed to fetch last game data: {e}")

    async def __getFixture(self):
        url = f"{K.BASE_URL}/fixtures"
        body = {"team": K.TEAM_ID, "next": 6}
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=K.headers, params=body)
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
        body = {"team": K.TEAM_ID}
        url = f"{K.BASE_URL}/leagues"
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=K.headers, params=body)
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
        url = f"{K.BASE_URL}/standings"
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=K.headers, params=body)
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

                            # Append the restructured data if not already present
                    if restructured_league["id"] not in self.processed_league_ids:
                        self.leaguesStandings.append(restructured_league)
                        self.processed_league_ids.add(restructured_league["id"])
            except httpx.RequestError as e:
                self.logger.error(
                    f"Failed to fetch standings data for league {leagueId}: {e}"
                )

    async def __check_for_tomorrow_games(self):
        tomorrow = (datetime.now(pytz.UTC) + timedelta(days=1)).date()

        for game in self.fixture:
            game_date = datetime.strptime(
                game["fixture"]["date"], "%Y-%m-%dT%H:%M:%S%z"
            ).date()

            if game_date == tomorrow:
                # Add a new document in Firestore for notifications
                notification_data = {
                    "es": "Mañana juega Boca",
                    "en": "Boca plays tomorrow",
                    "gameDate": game["fixture"]["date"],
                    "team2": game["teams"]["away"]["name"],
                }
                self.firestore_manager.add_data(
                    "game_alerts", game["fixture"]["id"], notification_data
                )
                self.logger.info(
                    f"Notification scheduled for game {game['teams']['home']['name']} vs {game['teams']['away']['name']} on {game_date}"
                )
