"""CLI for fantasy football playoff odds database."""

from __future__ import annotations

import argparse

from db import init_db, get_playoff_odds, search_players, get_league_count, get_roster_count
from sleeper_api import SleeperAPI
from collector import crawl_and_store_leagues, load_player_cache, clear_crawl_state


def cmd_init(args: argparse.Namespace) -> None:
    """Initialize the database."""
    init_db()
    print("Database initialized.")


def cmd_load_players(args: argparse.Namespace) -> None:
    """Load player data from Sleeper API."""
    init_db()
    api = SleeperAPI()
    load_player_cache(api, force_refresh=args.force)


def cmd_crawl(args: argparse.Namespace) -> None:
    """Crawl leagues starting from seed usernames."""
    init_db()
    
    if args.reset:
        clear_crawl_state()
        print("Cleared crawl state. Starting fresh.")
    
    api = SleeperAPI()

    # Load players first if not already loaded
    load_player_cache(api)

    crawl_and_store_leagues(
        api=api,
        seed_usernames=args.seeds,
        season=args.season,
        target_leagues=args.target,
        skip_existing=not args.force,
    )


def cmd_odds(args: argparse.Namespace) -> None:
    """Get playoff odds for a player."""
    init_db()

    # If query looks like a player ID (numeric), use it directly
    if args.query.isdigit():
        result = get_playoff_odds(args.query)
        print_odds_result(result)
    else:
        # Search for player by name
        matches = search_players(args.query)
        if not matches:
            print(f"No players found matching '{args.query}'")
            return

        if len(matches) == 1:
            result = get_playoff_odds(matches[0]["player_id"])
            print_odds_result(result)
        else:
            print(f"Found {len(matches)} players matching '{args.query}':")
            for p in matches[:10]:
                print(f"  {p['player_id']}: {p['full_name']} ({p['position']}, {p['team']})")
            print("\nRe-run with a player_id for specific odds.")


def print_odds_result(result: dict) -> None:
    """Pretty print playoff odds result."""
    if result["total_rosters"] == 0:
        print(f"No data found for player {result['player_id']}")
        return

    name = result["full_name"] or result["player_id"]
    pos = result["position"] or "?"
    team = result["team"] or "?"

    print(f"\n{name} ({pos}, {team})")
    print("-" * 40)
    print(f"Total rosters:   {result['total_rosters']}")
    print(f"Made playoffs:   {result['playoff_rosters']}")
    print(f"Playoff rate:    {result['playoff_pct']}%")
    print()


def cmd_stats(args: argparse.Namespace) -> None:
    """Show database statistics."""
    init_db()
    leagues = get_league_count()
    rosters = get_roster_count()
    print(f"Leagues: {leagues}")
    print(f"Rosters with playoff data: {rosters}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fantasy Football Playoff Odds Database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # init command
    subparsers.add_parser("init", help="Initialize the database")

    # load-players command
    load_p = subparsers.add_parser("load-players", help="Load player data from Sleeper")
    load_p.add_argument("--force", action="store_true", help="Force refresh from API")

    # crawl command
    crawl_p = subparsers.add_parser("crawl", help="Crawl leagues from seed usernames")
    crawl_p.add_argument("seeds", nargs="+", help="Seed usernames to start crawl from")
    crawl_p.add_argument("--season", type=int, default=2025, help="Season year (default: 2025)")
    crawl_p.add_argument("--target", type=int, default=100, help="Target number of leagues (default: 100)")
    crawl_p.add_argument("--force", action="store_true", help="Re-process existing leagues")
    crawl_p.add_argument("--reset", action="store_true", help="Clear saved state and start fresh")

    # odds command
    odds_p = subparsers.add_parser("odds", help="Get playoff odds for a player")
    odds_p.add_argument("query", help="Player name (search) or player_id")

    # stats command
    subparsers.add_parser("stats", help="Show database statistics")

    args = parser.parse_args()

    commands = {
        "init": cmd_init,
        "load-players": cmd_load_players,
        "crawl": cmd_crawl,
        "odds": cmd_odds,
        "stats": cmd_stats,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
