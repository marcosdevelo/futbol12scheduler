import httpx
from datetime import datetime, timedelta

import pytz

from firebase_manager import FirestoreManager
from k import K
from logging_config import configure_logging

current_year = datetime.now().year


class FootballFetcher:
    def __init__(self):
        self.leaguesStandings = []
        self.fixture = []
        self.lastGame = []
        self.topScorers = []
        self.logger = configure_logging()
        self.firestore_manager = FirestoreManager()
        self.processed_league_ids = set()
        self.processed_topscorer_league_ids = set()

    async def start(self):
        await self.resetData()
        await self.__getFixture()
        await self.__getStandings()
        await self.__getLastGameResults()
        await self.__getLastGameStatistics()
        await self.__getTopScorers()
        await self.__storeData()

    async def resetData(self):
        self.leaguesStandings = []
        self.fixture = []
        self.lastGame = []
        self.topScorers = []
        self.processed_league_ids.clear()
        self.processed_topscorer_league_ids.clear()

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
        if len(self.topScorers) > 0:
            newData["topScorers"] = self.topScorers
        try:
            self.firestore_manager.update_data(collection_name, document_id, newData)
            # await self.__check_for_tomorrow_games()
            self.logger.info("Data stored in Firestore.")
        except Exception as e:
            self.logger.error(f"Failed to store data in Firestore: {e}")

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
        body = {"team": K.TEAM_ID, "next": 16}
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

                    # Special handling for Liga Profesional Argentina
                    if leagueWithStandings["name"] == "Liga Profesional Argentina":
                        # Handle multiple standings arrays
                        for standings_group in leagueWithStandings["standings"]:
                            # Get group name from first team
                            group_name = standings_group[0]["group"] if standings_group else None

                            # Create a unique identifier combining league ID and group name
                            unique_id = f"{leagueWithStandings['id']}_{group_name}"

                            # Create a new league entry for each group
                            restructured_league = {
                                "id": leagueWithStandings["id"],
                                "name": group_name,  # Use group name instead of league name
                                "country": leagueWithStandings["country"],
                                "logo": leagueWithStandings["logo"],
                                "flag": leagueWithStandings["flag"],
                                "season": leagueWithStandings["season"],
                                "group": group_name,
                                "standings": standings_group
                            }

                            # Append the restructured data if not already present
                            if unique_id not in self.processed_league_ids:
                                self.leaguesStandings.append(restructured_league)
                                self.processed_league_ids.add(unique_id)
                    else:
                        # Original behavior for other leagues
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

    async def __getLastGameStatistics(self):
        body = {"team": K.TEAM_ID, "fixture": self.lastGame[0]['fixture']['id']}
        url = f"{K.BASE_URL}/fixtures/statistics"
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=K.headers, params=body)
                response.raise_for_status()
                data = response.json()
                
                # Add statistics to the lastGame property
                if len(self.lastGame) > 0 and len(data["response"]) > 0:
                    self.lastGame[0]["statistics"] = data["response"]
                    self.logger.info("Successfully fetched and added statistics to last game")
                else:
                    self.logger.warning("No statistics data available for the last game")
            except httpx.RequestError as e:
                self.logger.error(f"Failed to fetch last game statistics: {e}")
                # Initialize empty statistics if request fails
                if len(self.lastGame) > 0:
                    self.lastGame[0]["statistics"] = []

    async def __getTopScorers(self):
        if self.leaguesStandings:
            for league in self.leaguesStandings:
                # Skip if we've already processed this league's top scorers
                if league["id"] in self.processed_topscorer_league_ids:
                    continue
                
                body = {"league": league["id"], "season": current_year}
                url = f"{K.BASE_URL}/players/topscorers"
                
                async with httpx.AsyncClient() as client:
                    try:
                        response = await client.get(url, headers=K.headers, params=body)
                        response.raise_for_status()
                        data = response.json()
                        
                        if len(data["response"]) > 0:
                            # Handle special case for Argentine league
                            league_name = "Primera LPF" if league["id"] == 128 else league["name"]
                            
                            # Create a structured object for the league's top scorers
                            league_top_scorers = {
                                "league_id": league["id"],
                                "league_name": league_name,
                                "country": league["country"],
                                "logo": league["logo"],
                                "flag": league["flag"],
                                "season": current_year,
                                "scorers": data["response"]
                            }
                            
                            self.topScorers.append(league_top_scorers)
                            self.processed_topscorer_league_ids.add(league["id"])
                            self.logger.info(f"Successfully fetched top scorers for league {league_name}")
                    except httpx.RequestError as e:
                        self.logger.error(f"Failed to fetch top scorers for league {league['id']}: {e}") 