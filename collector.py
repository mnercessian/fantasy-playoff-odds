"""Data collection pipeline for crawling leagues and storing roster/playoff data."""

from __future__ import annotations

import json
import random
from collections import deque
from pathlib import Path

from db import (
    get_league_count,
    insert_league,
    insert_roster,
    insert_roster_players,
    insert_players_bulk,
    league_exists,
)
from sleeper_api import SleeperAPI, extract_playoff_roster_ids

STATE_FILE = Path(__file__).parent / "data" / "crawl_state.json"


def load_crawl_state() -> dict:
    """Load saved crawl state from disk."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"seen_user_ids": [], "user_queue": []}


def save_crawl_state(seen_user_ids: set[str], user_queue: deque[str]) -> None:
    """Save crawl state to disk for resuming later."""
    STATE_FILE.parent.mkdir(exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump({
            "seen_user_ids": list(seen_user_ids),
            "user_queue": list(user_queue),
        }, f)


def clear_crawl_state() -> None:
    """Clear saved crawl state."""
    if STATE_FILE.exists():
        STATE_FILE.unlink()


def crawl_and_store_leagues(
    api: SleeperAPI,
    seed_usernames: list[str],
    season: int,
    target_leagues: int = 1000,
    max_users_to_visit: int = 20000,
    max_leagues_per_user: int = 5,
    shuffle_queue: bool = True,
    skip_existing: bool = True,
) -> int:
    """
    Crawl Sleeper user<->league graph and store league/roster/playoff data.
    
    Args:
        api: SleeperAPI client instance
        seed_usernames: Starting usernames to crawl from
        season: NFL season year (e.g., 2025)
        target_leagues: Target number of leagues to collect
        max_users_to_visit: Maximum users to visit before stopping
        max_leagues_per_user: Max leagues to process per user (limits heavy users)
        shuffle_queue: Whether to randomize traversal order
        skip_existing: Skip leagues already in the database
    
    Returns:
        Number of new leagues processed
    """
    # Load saved state or start fresh
    state = load_crawl_state()
    seen_user_ids: set[str] = set(state["seen_user_ids"])
    user_q: deque[str] = deque(state["user_queue"])
    seen_leagues: set[str] = set()
    leagues_processed = 0

    # If no saved state, resolve seed usernames to user_ids
    if not user_q:
        print(f"Resolving {len(seed_usernames)} seed usernames...")
        for username in seed_usernames:
            user = api.get_user(username)
            if user and user.get("user_id"):
                user_q.append(user["user_id"])
    else:
        print(f"Resuming from saved state: {len(seen_user_ids)} users visited, {len(user_q)} in queue")

    print(f"Starting crawl targeting {target_leagues} new leagues...")

    while user_q and leagues_processed < target_leagues and len(seen_user_ids) < max_users_to_visit:
        if shuffle_queue and len(user_q) > 1:
            user_q.rotate(random.randint(0, len(user_q) - 1))

        user_id = user_q.popleft()
        if user_id in seen_user_ids:
            continue
        seen_user_ids.add(user_id)

        # Get user's leagues (API accepts user_id directly)
        leagues = api.get_user_leagues(user_id, season)
        print(f"  User {len(seen_user_ids)}: found {len(leagues)} leagues (queue: {len(user_q)}, processed: {leagues_processed})")

        leagues_from_this_user = 0
        for lg in leagues:
            league_id = str(lg.get("league_id", ""))
            if not league_id or league_id in seen_leagues:
                continue

            # Filter non-NFL
            if lg.get("sport") != "nfl":
                continue

            seen_leagues.add(league_id)

            # Skip if already in DB
            if skip_existing and league_exists(league_id):
                # Still enqueue users from existing leagues for discovery
                league_users = api.get_league_users(league_id)
                for u in league_users:
                    u_id = u.get("user_id")
                    if u_id and u_id not in seen_user_ids:
                        user_q.append(u_id)
                continue

            # Process this league
            success = process_league(api, league_id, season)
            if success:
                leagues_processed += 1
                leagues_from_this_user += 1
                # Enqueue league users for further crawling
                league_users = api.get_league_users(league_id)
                for u in league_users:
                    u_id = u.get("user_id")
                    if u_id and u_id not in seen_user_ids:
                        user_q.append(u_id)

            if leagues_processed >= target_leagues:
                break
            if leagues_from_this_user >= max_leagues_per_user:
                break

        # Save state periodically (every 10 users)
        if len(seen_user_ids) % 10 == 0:
            save_crawl_state(seen_user_ids, user_q)

    # Save state for resuming later
    save_crawl_state(seen_user_ids, user_q)
    print(f"Crawl complete. Processed {leagues_processed} new leagues, visited {len(seen_user_ids)} users.")
    print(f"State saved. {len(user_q)} users remaining in queue.")
    return leagues_processed


def process_league(api: SleeperAPI, league_id: str, season: int) -> bool:
    """
    Process a single league: fetch details, rosters, and playoff bracket.
    
    Returns True if successfully processed.
    """
    # Get league details
    league = api.get_league(league_id)
    if not league:
        return False

    # Only process leagues that have started (not pre_draft or drafting)
    status = league.get("status")
    if status in ("pre_draft", "drafting"):
        return False

    # Extract league info
    settings = league.get("settings", {})
    playoff_teams = settings.get("playoff_teams", 6)
    total_rosters = league.get("total_rosters", 12)

    # Skip leagues where >67% of teams make playoffs (unusual settings)
    if total_rosters > 0 and playoff_teams / total_rosters > 0.67:
        return False

    insert_league(
        league_id=league_id,
        season=str(season),
        name=league.get("name"),
        total_rosters=league.get("total_rosters"),
        playoff_teams=playoff_teams,
        status=status,
    )

    # Get rosters
    rosters = api.get_league_rosters(league_id)
    if not rosters:
        return False

    # Get winners bracket to determine playoff teams
    bracket = api.get_winners_bracket(league_id)
    playoff_roster_ids = extract_playoff_roster_ids(bracket)

    # Store rosters and their players
    for roster in rosters:
        roster_id = roster.get("roster_id")
        if roster_id is None:
            continue

        owner_id = roster.get("owner_id")
        players = roster.get("players") or []

        # Determine if this roster made playoffs
        made_playoffs = roster_id in playoff_roster_ids if playoff_roster_ids else None

        insert_roster(league_id, roster_id, owner_id, made_playoffs)
        insert_roster_players(league_id, roster_id, players)

    return True


def load_player_cache(api: SleeperAPI, force_refresh: bool = False) -> int:
    """
    Load all players from Sleeper API into the database.
    
    Returns number of players loaded.
    """
    players_dict = api.get_all_players(force_refresh=force_refresh)

    # Convert to list of dicts for bulk insert
    players_list = []
    for player_id, data in players_dict.items():
        # Build full name
        first = data.get("first_name", "")
        last = data.get("last_name", "")
        full_name = f"{first} {last}".strip() if first or last else None

        # Get primary position
        positions = data.get("fantasy_positions") or []
        position = positions[0] if positions else data.get("position")

        players_list.append({
            "player_id": player_id,
            "full_name": full_name,
            "position": position,
            "team": data.get("team"),
        })

    insert_players_bulk(players_list)
    print(f"Loaded {len(players_list)} players into database.")
    return len(players_list)

