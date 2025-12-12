"""Sleeper API client with rate limiting."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import requests

BASE = "https://api.sleeper.app/v1"
DATA_DIR = Path(__file__).parent / "data"
PLAYERS_CACHE_PATH = DATA_DIR / "players.json"

# Rate limiting: stay under 1000 calls/min (~0.06s min between calls)
# Using 0.05s gives ~1200 calls/min theoretical max, but real throughput is lower
DEFAULT_SLEEP = 0.05


class SleeperAPI:
    """Client for the Sleeper API with built-in rate limiting."""

    def __init__(self, sleep_s: float = DEFAULT_SLEEP):
        self.sleep_s = sleep_s
        self._last_call_time: float = 0

    def _rate_limit(self) -> None:
        """Ensure we don't exceed rate limits."""
        elapsed = time.time() - self._last_call_time
        if elapsed < self.sleep_s:
            time.sleep(self.sleep_s - elapsed)
        self._last_call_time = time.time()

    def _get(self, endpoint: str, timeout: int = 30) -> Any:
        """Make a GET request to the Sleeper API."""
        self._rate_limit()
        url = f"{BASE}{endpoint}"
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 429:
            raise RuntimeError(f"Rate limited (429) on {endpoint}. Slow down.")
        resp.raise_for_status()
        return resp.json()

    def get_user(self, username: str) -> dict[str, Any] | None:
        """Get user by username. Returns None if not found."""
        try:
            return self._get(f"/user/{username}")
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise

    def get_user_leagues(self, user_id: str, season: int) -> list[dict[str, Any]]:
        """Get all NFL leagues for a user in a given season."""
        try:
            return self._get(f"/user/{user_id}/leagues/nfl/{season}")
        except requests.HTTPError:
            return []

    def get_league(self, league_id: str) -> dict[str, Any] | None:
        """Get league details."""
        try:
            return self._get(f"/league/{league_id}")
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise

    def get_league_users(self, league_id: str) -> list[dict[str, Any]]:
        """Get all users in a league."""
        try:
            return self._get(f"/league/{league_id}/users")
        except requests.HTTPError:
            return []

    def get_league_rosters(self, league_id: str) -> list[dict[str, Any]]:
        """Get all rosters in a league."""
        try:
            return self._get(f"/league/{league_id}/rosters")
        except requests.HTTPError:
            return []

    def get_winners_bracket(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get the winners (playoff) bracket for a league.
        
        Returns list of matchup objects, each containing:
        - r: roster_id of one team
        - r2: roster_id of other team (if set)
        - w: winner roster_id (if determined)
        - l: loser roster_id (if determined)
        - m: matchup_id
        """
        try:
            return self._get(f"/league/{league_id}/winners_bracket")
        except requests.HTTPError:
            return []

    def get_all_players(self, force_refresh: bool = False) -> dict[str, dict[str, Any]]:
        """
        Get all NFL players. Caches locally since this is a ~5MB response.
        
        Returns dict keyed by player_id.
        """
        DATA_DIR.mkdir(exist_ok=True)

        if not force_refresh and PLAYERS_CACHE_PATH.exists():
            with open(PLAYERS_CACHE_PATH) as f:
                return json.load(f)

        print("Fetching all players from Sleeper API (this is ~5MB, may take a moment)...")
        players = self._get("/players/nfl")

        with open(PLAYERS_CACHE_PATH, "w") as f:
            json.dump(players, f)

        print(f"Cached {len(players)} players to {PLAYERS_CACHE_PATH}")
        return players


def extract_playoff_roster_ids(bracket: list[dict[str, Any]] | None) -> set[int]:
    """
    Extract all roster_ids that appear in the winners bracket.
    
    Any roster_id appearing in the bracket made the playoffs.
    Bracket structure: 't1' and 't2' are the roster_ids, 'r' is the round number.
    """
    roster_ids: set[int] = set()
    if not bracket:
        return roster_ids
    for matchup in bracket:
        # 't1' and 't2' are the roster_ids (teams) in each matchup
        if "t1" in matchup and matchup["t1"] is not None:
            roster_ids.add(matchup["t1"])
        if "t2" in matchup and matchup["t2"] is not None:
            roster_ids.add(matchup["t2"])
    return roster_ids

