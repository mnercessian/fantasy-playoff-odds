"""SQLite database schema and queries for playoff odds tracking."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

DATA_DIR = Path(__file__).parent / "data"
DB_PATH = DATA_DIR / "playoff_odds.db"


def get_connection() -> sqlite3.Connection:
    """Get a connection to the database, creating it if necessary."""
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Initialize the database schema."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS leagues (
            league_id TEXT PRIMARY KEY,
            season TEXT NOT NULL,
            name TEXT,
            total_rosters INTEGER,
            playoff_teams INTEGER,
            status TEXT
        );

        CREATE TABLE IF NOT EXISTS rosters (
            league_id TEXT NOT NULL,
            roster_id INTEGER NOT NULL,
            owner_id TEXT,
            made_playoffs INTEGER,  -- 0 or 1
            PRIMARY KEY (league_id, roster_id),
            FOREIGN KEY (league_id) REFERENCES leagues(league_id)
        );

        CREATE TABLE IF NOT EXISTS roster_players (
            league_id TEXT NOT NULL,
            roster_id INTEGER NOT NULL,
            player_id TEXT NOT NULL,
            PRIMARY KEY (league_id, roster_id, player_id),
            FOREIGN KEY (league_id, roster_id) REFERENCES rosters(league_id, roster_id)
        );

        CREATE TABLE IF NOT EXISTS players (
            player_id TEXT PRIMARY KEY,
            full_name TEXT,
            position TEXT,
            team TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_roster_players_player ON roster_players(player_id);
        CREATE INDEX IF NOT EXISTS idx_rosters_made_playoffs ON rosters(made_playoffs);
    """)

    conn.commit()
    conn.close()


def insert_league(league_id: str, season: str, name: str | None, total_rosters: int | None, 
                  playoff_teams: int | None, status: str | None) -> None:
    """Insert or replace a league record."""
    conn = get_connection()
    conn.execute(
        """INSERT OR REPLACE INTO leagues (league_id, season, name, total_rosters, playoff_teams, status)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (league_id, season, name, total_rosters, playoff_teams, status)
    )
    conn.commit()
    conn.close()


def insert_roster(league_id: str, roster_id: int, owner_id: str | None, made_playoffs: bool | None) -> None:
    """Insert or replace a roster record."""
    conn = get_connection()
    made_playoffs_int = None if made_playoffs is None else (1 if made_playoffs else 0)
    conn.execute(
        """INSERT OR REPLACE INTO rosters (league_id, roster_id, owner_id, made_playoffs)
           VALUES (?, ?, ?, ?)""",
        (league_id, roster_id, owner_id, made_playoffs_int)
    )
    conn.commit()
    conn.close()


def insert_roster_players(league_id: str, roster_id: int, player_ids: list[str]) -> None:
    """Insert player IDs for a roster (bulk insert)."""
    conn = get_connection()
    conn.executemany(
        """INSERT OR IGNORE INTO roster_players (league_id, roster_id, player_id)
           VALUES (?, ?, ?)""",
        [(league_id, roster_id, pid) for pid in player_ids]
    )
    conn.commit()
    conn.close()


def insert_players_bulk(players: list[dict[str, Any]]) -> None:
    """Bulk insert player records."""
    conn = get_connection()
    conn.executemany(
        """INSERT OR REPLACE INTO players (player_id, full_name, position, team)
           VALUES (?, ?, ?, ?)""",
        [(p["player_id"], p.get("full_name"), p.get("position"), p.get("team")) for p in players]
    )
    conn.commit()
    conn.close()


def get_playoff_odds(player_id: str) -> dict[str, Any]:
    """
    Get playoff odds for a specific player.
    
    Returns dict with:
        - player_id
        - full_name
        - total_rosters: how many rosters had this player
        - playoff_rosters: how many of those made playoffs
        - playoff_pct: percentage that made playoffs
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get player info
    cursor.execute("SELECT full_name, position, team FROM players WHERE player_id = ?", (player_id,))
    player_row = cursor.fetchone()

    # Get roster counts
    cursor.execute("""
        SELECT 
            COUNT(*) as total_rosters,
            SUM(CASE WHEN r.made_playoffs = 1 THEN 1 ELSE 0 END) as playoff_rosters
        FROM roster_players rp
        JOIN rosters r ON rp.league_id = r.league_id AND rp.roster_id = r.roster_id
        WHERE rp.player_id = ?
          AND r.made_playoffs IS NOT NULL
    """, (player_id,))
    
    counts = cursor.fetchone()
    conn.close()

    total = counts["total_rosters"] or 0
    playoffs = counts["playoff_rosters"] or 0
    pct = (playoffs / total * 100) if total > 0 else 0.0

    return {
        "player_id": player_id,
        "full_name": player_row["full_name"] if player_row else None,
        "position": player_row["position"] if player_row else None,
        "team": player_row["team"] if player_row else None,
        "total_rosters": total,
        "playoff_rosters": playoffs,
        "playoff_pct": round(pct, 2),
    }


def search_players(query: str, limit: int = 20) -> list[dict[str, Any]]:
    """Search for players by name (case-insensitive partial match)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT player_id, full_name, position, team 
           FROM players 
           WHERE full_name LIKE ? 
           ORDER BY full_name
           LIMIT ?""",
        (f"%{query}%", limit)
    )
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_league_count() -> int:
    """Get total number of leagues in the database."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM leagues")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def get_roster_count() -> int:
    """Get total number of rosters in the database."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM rosters WHERE made_playoffs IS NOT NULL")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def league_exists(league_id: str) -> bool:
    """Check if a league already exists in the database."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM leagues WHERE league_id = ?", (league_id,))
    exists = cursor.fetchone() is not None
    conn.close()
    return exists

