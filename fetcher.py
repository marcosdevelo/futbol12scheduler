import requests
from datetime import datetime
API_KEY = "e622b1469cdad8fb39da43fde1490356"
BASE_URL = "https://v3.football.api-sports.io/leagues?team=451"
TEAM_ID = 451
headers = {
    'x-apisports-key': API_KEY
}
class FootballFetcher:
    def __init__(self):
        self.standings = []
        self.fixture = []


    def startFetchRoutine(self):
        self.getFixture()
        self.getStandings()

    def getFixture(self):
        games_in_advance = 6
        url = f"{BASE_URL}/fixtures?team={TEAM_ID}&next={games_in_advance}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            self.fixture = data.response
        else:
            print(f"Failed to fetch data: {response.status_code}")

    def getStandings(self):
        current_year = datetime.now().year
        leagues = self.getLeagues()
        if leagues:
            for league in leagues:
                self.getLeagueStandings(league.league.id,current_year)

    def getLeagues(self):
        url = f"{BASE_URL}/leagues?team={TEAM_ID}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            # Process and map data to Firestore here
            print("Fetched data:", data)
            return data
        else:
            print(f"Failed to fetch data: {response.status_code}")

    def getLeagueStandings(self, leagueId, season):
        url = f"{BASE_URL}/standings?league={leagueId}&season={season}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            self.standings.append(data)
            print("Fetched data:", data)
        else:
            print(f"Failed to fetch data: {response.status_code}")

