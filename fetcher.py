import asyncio
from datetime import datetime

import httpx

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
        self.errors = []  # Store errors to return in response

    async def start(self):
        try:
            await self.resetData()
            await self.__getFixture()
            await self.__getStandings()
            await self.__getLastGameResults()
            await self.__getLastGameStatistics()
            await self.__getLastGameEvents()
            await self.__getLastGameLineups()
            await self.__getTopScorers()
            await self.__storeData()

            # Return success response with any errors that occurred
            return {
                "status": "success",
                "message": "Data fetched and stored successfully",
                "errors": self.errors if self.errors else None,
                "data_summary": {
                    "leaguesStandings_count": len(self.leaguesStandings),
                    "fixture_count": len(self.fixture),
                    "lastGame_count": len(self.lastGame),
                    "topScorers_count": len(self.topScorers)
                }
            }

        except Exception as e:
            error_msg = f"Critical error in FootballFetcher.start(): {str(e)}"
            self.logger.error(error_msg)
            self.errors.append(error_msg)
            return {
                "status": "error",
                "message": "Failed to fetch and store data",
                "errors": self.errors
            }

    async def resetData(self):
        self.leaguesStandings = []
        self.fixture = []
        self.lastGame = []
        self.topScorers = []
        self.processed_league_ids.clear()
        self.errors.clear()

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
            # Get existing data to preserve "Tabla Anual" entries and enabled/order properties
            existing_data = self.firestore_manager.read_data(collection_name, document_id)
            preserved_tabla_anual = []
            existing_standings_metadata = {}
            
            if existing_data and "leaguesStandings" in existing_data:
                for standing in existing_data["leaguesStandings"]:
                    if isinstance(standing, dict):
                        if standing.get("name") == "Tabla Anual":
                            preserved_tabla_anual.append(standing)
                            self.logger.info("Preserving existing 'Tabla Anual' standings entry")
                        else:
                            # Store metadata for existing standings (enabled and order properties)
                            standing_id = standing.get("id")
                            if standing_id:
                                existing_standings_metadata[standing_id] = {
                                    "enabled": standing.get("enabled", True),
                                    "order": standing.get("order", 0)
                                }

            # Check if we have valid new data to store
            has_valid_data = (
                len(self.leaguesStandings) > 0 or 
                len(self.fixture) > 0 or 
                len(self.lastGame) > 0 or 
                len(self.topScorers) > 0
            )

            if has_valid_data:
                # Prepare new standings data with preserved enabled/order properties
                new_standings = self.__clean_data_for_firestore(self.leaguesStandings, "leaguesStandings")
                
                # Preserve enabled and order properties from existing data
                for standing in new_standings:
                    if isinstance(standing, dict):
                        standing_id = standing.get("id")
                        if standing_id in existing_standings_metadata:
                            # Preserve existing enabled and order values
                            standing["enabled"] = existing_standings_metadata[standing_id]["enabled"]
                            standing["order"] = existing_standings_metadata[standing_id]["order"]
                            self.logger.info(f"Preserved enabled={standing['enabled']} and order={standing['order']} for league {standing_id}")
                        else:
                            # Set default values for new standings
                            standing["enabled"] = True
                            standing["order"] = 0
                            self.logger.info(f"Set default enabled=True and order=0 for new league {standing_id}")
                
                # Add preserved "Tabla Anual" entries to the new standings
                if preserved_tabla_anual:
                    new_standings.extend(preserved_tabla_anual)
                    self.logger.info(f"Added {len(preserved_tabla_anual)} preserved 'Tabla Anual' entries to standings")

                # Store new data with preserved "Tabla Anual"
                newData["leaguesStandings"] = new_standings
                newData["fixture"] = self.__clean_data_for_firestore(self.fixture, "fixture")
                newData["lastGame"] = self.__clean_data_for_firestore(self.lastGame, "lastGame")
                newData["topScorers"] = self.__clean_data_for_firestore(self.topScorers, "topScorers")
                
                self.logger.info("Storing new data with preserved 'Tabla Anual' entries and enabled/order properties")
            else:
                # If no valid new data, preserve existing data and only update "Tabla Anual" if needed
                if existing_data:
                    newData = existing_data.copy()
                    
                    # Only preserve "Tabla Anual" if it doesn't already exist
                    existing_tabla_anual = False
                    if "leaguesStandings" in newData:
                        for standing in newData["leaguesStandings"]:
                            if isinstance(standing, dict) and standing.get("name") == "Tabla Anual":
                                existing_tabla_anual = True
                                break
                    
                    if not existing_tabla_anual and preserved_tabla_anual:
                        if "leaguesStandings" not in newData:
                            newData["leaguesStandings"] = []
                        newData["leaguesStandings"].extend(preserved_tabla_anual)
                        self.logger.info(f"Added {len(preserved_tabla_anual)} preserved 'Tabla Anual' entries to existing data")
                else:
                    # No existing data and no new data, create empty structure
                    newData = {
                        "leaguesStandings": preserved_tabla_anual,
                        "fixture": [],
                        "lastGame": [],
                        "topScorers": []
                    }
                    if preserved_tabla_anual:
                        self.logger.info(f"Created new document with {len(preserved_tabla_anual)} preserved 'Tabla Anual' entries")
                
                self.logger.info("No valid new data available, preserving existing data")

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
            self.__log_and_store_error(
                "Firestore Storage Error",
                str(e),
                "Failed to store data in Firestore"
            )

    async def __getLastGameResults(self):
        url = f"{K.BASE_URL}/fixtures"
        body = {"team": K.TEAM_ID, "last": 1}

        data = await self.__make_api_request_with_retry(url, body)
        if data is not None:
            self.lastGame = data["response"]
        else:
            self.__log_and_store_error(
                "Last Game Data Error",
                "Failed to fetch last game data after all retries",
                f"Team ID: {K.TEAM_ID}"
            )
            self.lastGame = []

    async def __getFixturePredictions(self, fixture_id):
        url = f"{K.BASE_URL}/predictions"
        body = {"fixture": fixture_id}

        data = await self.__make_api_request_with_retry(url, body)
        if data is not None and len(data["response"]) > 0:
            return data["response"][0]
        return None

    async def __getFixture(self):
        url = f"{K.BASE_URL}/fixtures"
        body = {"team": K.TEAM_ID, "next": 8}

        data = await self.__make_api_request_with_retry(url, body)
        if data is not None:
            self.fixture = data["response"]

            # Get predictions for the next game (first fixture)
            if len(self.fixture) > 0:
                next_game_predictions = await self.__getFixturePredictions(self.fixture[0]["fixture"]["id"])
                if next_game_predictions:
                    self.fixture[0]["predictions"] = next_game_predictions
        else:
            self.__log_and_store_error(
                "Fixture Data Error",
                "Failed to fetch fixture data after all retries",
                f"Team ID: {K.TEAM_ID}"
            )
            self.fixture = []

    async def __getStandings(self):
        leagues = await self.__getLeagues()
        if leagues:
            for league in leagues:
                await self.__getLeagueStandings(league["league"]["id"], current_year)

    async def __getLeagues(self):
        body = {"team": K.TEAM_ID}
        url = f"{K.BASE_URL}/leagues"

        data = await self.__make_api_request_with_retry(url, body)
        if data is not None:
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
        else:
            self.__log_and_store_error(
                "Leagues Data Error",
                "Failed to fetch leagues data after all retries",
                f"Team ID: {K.TEAM_ID}"
            )
            return None

    async def __getLeagueStandings(self, leagueId, season):
        body = {"league": leagueId, "season": season}
        url = f"{K.BASE_URL}/standings"

        data = await self.__make_api_request_with_retry(url, body)
        if data is not None and len(data["response"]) > 0:
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
                        "enabled": True,  # Default value, will be overridden if exists in Firebase
                        "order": 0,  # Default value, will be overridden if exists in Firebase
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
                    "enabled": True,  # Default value, will be overridden if exists in Firebase
                    "order": 0,  # Default value, will be overridden if exists in Firebase
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
        else:
            self.__log_and_store_error(
                "Standings Data Error",
                f"Failed to fetch standings data for league {leagueId} after all retries",
                f"League ID: {leagueId}, Season: {season}"
            )

    async def __getLastGameStatistics(self):
        if len(self.lastGame) == 0:
            self.logger.warning("No last game data available for statistics")
            return

        body = {"fixture": self.lastGame[0]['fixture']['id']}
        url = f"{K.BASE_URL}/fixtures/statistics"

        data = await self.__make_api_request_with_retry(url, body)
        if data is not None and len(data["response"]) > 0:
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
            if len(self.lastGame) > 0:
                self.lastGame[0]["statistics"] = []

    async def __getLastGameEvents(self):
        if len(self.lastGame) == 0:
            self.logger.warning("No last game data available for events")
            return

        url = f"{K.BASE_URL}/fixtures/events"
        body = {"fixture": self.lastGame[0]["fixture"]["id"]}

        data = await self.__make_api_request_with_retry(url, body)
        if data is not None and len(data["response"]) > 0:
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

    async def __getLastGameLineups(self):
        if len(self.lastGame) == 0:
            self.logger.warning("No last game data available for lineups")
            return

        url = f"{K.BASE_URL}/fixtures/lineups"
        body = {"fixture": self.lastGame[0]["fixture"]["id"]}

        data = await self.__make_api_request_with_retry(url, body)
        if data is not None and len(data["response"]) > 0:
            self.lastGame[0]["lineups"] = data["response"]
            self.logger.info("Successfully fetched last game lineups")
        else:
            self.lastGame[0]["lineups"] = []
            self.logger.info("No lineups found in response")

    async def __make_api_request_with_retry(self, url, params, max_retries=3, base_delay=6, timeout=30):
        """
        Make API request with exponential backoff for rate limiting
        """
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.get(url, headers=K.headers, params=params)

                    # Check for rate limit error
                    if response.status_code == 405:
                        error_data = response.json()
                        if 'errors' in error_data and 'rateLimit' in error_data['errors']:
                            self.__log_and_store_error(
                                "Rate Limit Error",
                                error_data['errors']['rateLimit'],
                                f"URL: {url}, Attempt: {attempt + 1}/{max_retries}"
                            )

                            if attempt < max_retries - 1:
                                # Calculate delay with exponential backoff
                                delay = base_delay * (2 ** attempt)
                                self.logger.info(f"Waiting {delay} seconds before retry...")
                                await asyncio.sleep(delay)
                                continue
                            else:
                                self.__log_and_store_error(
                                    "Max Retries Reached",
                                    "Rate limit exceeded after all retry attempts",
                                    f"URL: {url}"
                                )
                                return None

                    response.raise_for_status()
                    return response.json()

            except httpx.TimeoutException as e:
                self.__log_and_store_error(
                    "Timeout Error",
                    f"Request timed out after {timeout}s: {str(e)}",
                    f"URL: {url}, Attempt: {attempt + 1}/{max_retries}"
                )
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    self.logger.info(f"Timeout occurred, waiting {delay} seconds before retry...")
                    await asyncio.sleep(delay)
                else:
                    return None
            except httpx.RequestError as e:
                self.__log_and_store_error(
                    "Request Error",
                    str(e),
                    f"URL: {url}, Attempt: {attempt + 1}/{max_retries}"
                )
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    await asyncio.sleep(delay)
                else:
                    return None
            except Exception as e:
                self.__log_and_store_error(
                    "Unexpected Error",
                    str(e),
                    f"URL: {url}, Attempt: {attempt + 1}/{max_retries}"
                )
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    await asyncio.sleep(delay)
                else:
                    return None

        return None

    async def __getTopScorers(self):
        # Hard code to only fetch top scorers for league 128 (Primera LPF)
        league_id = 128
        
        # Find the first occurrence of this league in standings for metadata
        league_metadata = None
        for league in self.leaguesStandings:
            if league["id"] == league_id:
                league_metadata = league
                break
        
        if not league_metadata:
            self.logger.warning(f"Could not find metadata for league {league_id} in standings")
            return
        
        body = {"league": league_id, "season": current_year}
        url = f"{K.BASE_URL}/players/topscorers"

        self.logger.info(f"Fetching top scorers for league {league_id} - Primera LPF with 60s timeout")

        # Use the rate-limited request method with extended timeout for top scorers
        data = await self.__make_api_request_with_retry(url, body, max_retries=3, base_delay=6, timeout=60)

        if data is None:
            self.__log_and_store_error(
                "Top Scorers Error",
                f"Failed to fetch top scorers for league {league_id} after all retries",
                f"League ID: {league_id}, League Name: Primera LPF"
            )
            # Set empty top scorers to avoid breaking the data structure
            self.topScorers = []
            return

        # Check for rate limit errors in the response
        if data.get("errors") and "rateLimit" in data["errors"]:
            self.logger.warning(f"Rate limit hit for league {league_id}: {data['errors']['rateLimit']}")
            return

        if len(data.get("response", [])) > 0:
            # Create a structured object for the league's top scorers
            league_top_scorers = {
                "league_id": league_id,
                "league_name": "Primera LPF",
                "country": league_metadata["country"],
                "logo": league_metadata["logo"],
                "flag": league_metadata["flag"],
                "season": current_year,
                "scorers": data["response"]
            }

            self.topScorers.append(league_top_scorers)
            self.logger.info(f"✅ Successfully fetched {len(data['response'])} top scorers for Primera LPF")
        else:
            self.logger.warning(f"No top scorers data available for league {league_id} - skipping entry to avoid empty scorers array")

    def __log_and_store_error(self, error_type, error_message, context=""):
        """
        Log error and store it for response
        """
        full_message = f"{error_type}: {error_message}"
        if context:
            full_message += f" | Context: {context}"

        self.logger.error(full_message)
        self.errors.append(full_message)
