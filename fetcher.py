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
        await self.__getLastGameEvents()
        await self.__getLastGameLineups()
        await self.__getTopScorers()
        await self.__storeData()

    async def resetData(self):
        self.leaguesStandings = []
        self.fixture = []
        self.lastGame = []
        self.topScorers = []
        self.processed_league_ids.clear()
        self.processed_topscorer_league_ids.clear()

    def __clean_data_for_firestore(self, data, path=""):
        """
        Clean and validate data before storing in Firestore.
        Ensures data structure matches Flutter app expectations and never returns null values.
        """
        try:
            if isinstance(data, dict):
                cleaned_dict = {}
                for k, v in data.items():
                    # Skip empty keys
                    if not k or not isinstance(k, str):
                        continue
                    # Clean the key
                    k = str(k).strip()
                    if not k:
                        continue
                    
                    if v is not None:
                        try:
                            cleaned_value = self.__clean_data_for_firestore(v, f"{path}.{k}")
                            if cleaned_value is not None:
                                cleaned_dict[k] = cleaned_value
                        except Exception as e:
                            self.logger.warning(f"Error cleaning dict value at {path}.{k}: {str(e)}")
                            cleaned_dict[k] = str(v)
                return cleaned_dict if cleaned_dict else {}
            elif isinstance(data, list):
                # Always keep arrays as arrays
                cleaned_list = []
                for item in data:
                    if item is not None:
                        cleaned_item = self.__clean_data_for_firestore(item, f"{path}[{len(cleaned_list)}]")
                        if cleaned_item is not None:
                            cleaned_list.append(cleaned_item)
                return cleaned_list
            elif isinstance(data, (str, int, float, bool)):
                # Convert to string if it's a number or boolean
                if isinstance(data, (int, float, bool)):
                    return str(data)
                # Clean string
                cleaned_str = str(data).strip()
                return cleaned_str if cleaned_str else ""
            elif data is None:
                # Return empty list or dict based on context
                if path.endswith(("lastGame", "fixture", "leaguesStandings", "topScorers")):
                    return []
                return {}
            else:
                self.logger.warning(f"Converting non-serializable value at {path} to string: {type(data)}")
                return str(data)
        except Exception as e:
            self.logger.error(f"Error in __clean_data_for_firestore at {path}: {str(e)}")
            # Return empty list or dict based on context
            if path.endswith(("lastGame", "fixture", "leaguesStandings", "topScorers")):
                return []
            return {}

    async def __storeData(self):
        collection_name = K.COLLECTION_NAME
        document_id = K.COLLECTION_NAME
        newData = {}

        try:
            # Always include all fields with at least empty lists
            newData["leaguesStandings"] = self.__clean_data_for_firestore(self.leaguesStandings, "leaguesStandings")
            newData["fixture"] = self.__clean_data_for_firestore(self.fixture, "fixture")
            newData["lastGame"] = self.__clean_data_for_firestore(self.lastGame, "lastGame")
            newData["topScorers"] = self.__clean_data_for_firestore(self.topScorers, "topScorers")

            # Log the structure of the data before storing
            self.logger.info("Data structure before storing:")
            for key in newData:
                self.logger.info(f"{key} type: {type(newData[key])}")
                if isinstance(newData[key], list):
                    self.logger.info(f"{key} length: {len(newData[key])}")
                    if len(newData[key]) > 0:
                        self.logger.info(f"{key}[0] type: {type(newData[key][0])}")
                        if isinstance(newData[key][0], dict):
                            self.logger.info(f"{key}[0] keys: {list(newData[key][0].keys())}")

            self.firestore_manager.update_data(collection_name, document_id, newData)
            self.logger.info("Data stored in Firestore.")
        except Exception as e:
            self.logger.error(f"Failed to store data in Firestore: {e}")
            # Log the problematic data structure
            self.logger.error("Problematic data structure:")
            for key in newData:
                self.logger.error(f"{key}: {type(newData[key])}")
                if isinstance(newData[key], list):
                    self.logger.error(f"{key} length: {len(newData[key])}")

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

    async def __getFixturePredictions(self, fixture_id):
        url = f"{K.BASE_URL}/predictions"
        body = {"fixture": fixture_id}

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=K.headers, params=body)
                response.raise_for_status()
                data = response.json()

                if len(data["response"]) > 0:
                    return data["response"][0]
                return None
            except httpx.RequestError as e:
                self.logger.error(f"Failed to fetch predictions for fixture {fixture_id}: {e}")
                return None

    async def __getFixture(self):
        url = f"{K.BASE_URL}/fixtures"
        body = {"team": K.TEAM_ID, "next": 8}
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=K.headers, params=body)
                response.raise_for_status()
                data = response.json()
                self.fixture = data["response"]

                # Get predictions for the next game (first fixture)
                if len(self.fixture) > 0:
                    next_game_predictions = await self.__getFixturePredictions(self.fixture[0]["fixture"]["id"])
                    if next_game_predictions:
                        self.fixture[0]["predictions"] = next_game_predictions
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
        body = {"fixture": self.lastGame[0]['fixture']['id']}
        url = f"{K.BASE_URL}/fixtures/statistics"

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=K.headers, params=body)
                response.raise_for_status()
                data = response.json()

                # Add statistics to the lastGame property
                if len(self.lastGame) > 0 and len(data["response"]) > 0:
                    # Ensure all numeric values in statistics are strings
                    cleaned_statistics = []
                    for stat in data["response"]:
                        cleaned_stat = {}
                        for key, value in stat.items():
                            if isinstance(value, (int, float)):
                                cleaned_stat[key] = str(value)
                            else:
                                cleaned_stat[key] = value
                        cleaned_statistics.append(cleaned_stat)
                    
                    self.lastGame[0]["statistics"] = cleaned_statistics
                    self.logger.info("Successfully fetched and added statistics to last game")
                else:
                    self.logger.warning("No statistics data available for the last game")
            except httpx.RequestError as e:
                self.logger.error(f"Failed to fetch last game statistics: {e}")
                # Initialize empty statistics if request fails
                if len(self.lastGame) > 0:
                    self.lastGame[0]["statistics"] = []

    async def __getLastGameEvents(self):
        if len(self.lastGame) > 0:
            url = f"{K.BASE_URL}/fixtures/events"
            body = {"fixture": self.lastGame[0]["fixture"]["id"]}

            async with httpx.AsyncClient() as client:
                try:
                    response = await client.get(url, headers=K.headers, params=body)
                    response.raise_for_status()
                    data = response.json()

                    if len(data["response"]) > 0:
                        # Clean and convert time values to integers
                        cleaned_events = []
                        for event in data["response"]:
                            try:
                                cleaned_event = {}
                                # Copy all fields except time
                                for key, value in event.items():
                                    if key != 'time':
                                        cleaned_event[key] = value
                                
                                # Handle time field separately
                                if 'time' in event and isinstance(event['time'], dict):
                                    time_dict = event['time']
                                    elapsed = time_dict.get('elapsed')
                                    extra = time_dict.get('extra')
                                    
                                    # Convert elapsed to int, default to 0 if conversion fails
                                    try:
                                        elapsed_int = int(float(str(elapsed))) if elapsed is not None else 0
                                    except (ValueError, TypeError):
                                        elapsed_int = 0
                                        self.logger.warning(f"Failed to convert elapsed time: {elapsed}")
                                    
                                    # Convert extra to int if it exists, otherwise None
                                    try:
                                        extra_int = int(float(str(extra))) if extra is not None else None
                                    except (ValueError, TypeError):
                                        extra_int = None
                                        self.logger.warning(f"Failed to convert extra time: {extra}")
                                    
                                    cleaned_event['time'] = {
                                        'elapsed': elapsed_int,
                                        'extra': extra_int
                                    }
                                else:
                                    cleaned_event['time'] = {'elapsed': 0, 'extra': None}
                                
                                cleaned_events.append(cleaned_event)
                                self.logger.debug(f"Processed event with time: {cleaned_event['time']}")
                            except Exception as e:
                                self.logger.error(f"Error processing event: {str(e)}")
                                continue
                        
                        self.lastGame[0]["events"] = cleaned_events
                        self.logger.info(f"Successfully fetched and processed {len(cleaned_events)} events")
                    else:
                        self.lastGame[0]["events"] = []
                        self.logger.info("No events found in response")
                except httpx.RequestError as e:
                    self.logger.error(f"Failed to fetch last game events: {e}")
                    self.lastGame[0]["events"] = []

    async def __getLastGameLineups(self):
        if len(self.lastGame) > 0:
            url = f"{K.BASE_URL}/fixtures/lineups"
            body = {"fixture": self.lastGame[0]["fixture"]["id"]}

            async with httpx.AsyncClient() as client:
                try:
                    response = await client.get(url, headers=K.headers, params=body)
                    response.raise_for_status()
                    data = response.json()

                    if len(data["response"]) > 0:
                        self.lastGame[0]["lineups"] = data["response"]
                        self.logger.info("Successfully fetched last game lineups")
                    else:
                        self.lastGame[0]["lineups"] = []
                except httpx.RequestError as e:
                    self.logger.error(f"Failed to fetch last game lineups: {e}")
                    self.lastGame[0]["lineups"] = []

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
