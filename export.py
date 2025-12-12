"""Export playoff odds data to JSON for the web interface."""

from __future__ import annotations

import json
from pathlib import Path

from db import get_connection, init_db

VALID_POSITIONS = {"QB", "RB", "WR", "TE", "K", "DEF"}


def export_all_player_odds(min_roster_pct: float = 1.0) -> list[dict]:
    """
    Export playoff odds for all players with sufficient sample size.
    
    Args:
        min_roster_pct: Minimum % of rosters a player must be on to include
        
    Returns:
        List of player dicts with playoff odds
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get total roster count first
    cursor.execute("SELECT COUNT(*) FROM rosters WHERE made_playoffs IS NOT NULL")
    total_rosters_in_db = cursor.fetchone()[0]
    min_rosters = int(total_rosters_in_db * min_roster_pct / 100)

    cursor.execute("""
        SELECT 
            p.player_id,
            p.full_name,
            p.position,
            p.team,
            COUNT(*) as total_rosters,
            SUM(CASE WHEN r.made_playoffs = 1 THEN 1 ELSE 0 END) as playoff_rosters
        FROM players p
        JOIN roster_players rp ON p.player_id = rp.player_id
        JOIN rosters r ON rp.league_id = r.league_id AND rp.roster_id = r.roster_id
        WHERE r.made_playoffs IS NOT NULL
          AND p.position IN ('QB', 'RB', 'WR', 'TE', 'K', 'DEF')
        GROUP BY p.player_id
        HAVING COUNT(*) >= ?
        ORDER BY playoff_rosters * 1.0 / COUNT(*) DESC
    """, (min_rosters,))

    results = []
    for row in cursor.fetchall():
        total = row["total_rosters"]
        playoffs = row["playoff_rosters"]
        pct = round(playoffs / total * 100, 2) if total > 0 else 0
        ownership_pct = round(total / total_rosters_in_db * 100, 1)

        results.append({
            "player_id": row["player_id"],
            "name": row["full_name"] or row["player_id"],
            "position": row["position"],
            "team": row["team"] or "FA",
            "total_rosters": total,
            "playoff_rosters": playoffs,
            "playoff_pct": pct,
            "ownership_pct": ownership_pct,
        })

    conn.close()
    return results


def get_baseline_playoff_rate() -> float:
    """Get the overall playoff rate across all leagues."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT SUM(CASE WHEN made_playoffs = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
        FROM rosters WHERE made_playoffs IS NOT NULL
    """)
    rate = cursor.fetchone()[0] or 0
    conn.close()
    return round(rate, 2)


def get_stats() -> dict:
    """Get overall database statistics."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM leagues")
    leagues = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM rosters WHERE made_playoffs IS NOT NULL")
    rosters = cursor.fetchone()[0]
    
    conn.close()
    return {"leagues": leagues, "rosters": rosters}


def generate_standalone_html(output_path: Path, data: dict) -> None:
    """Generate a standalone HTML file with embedded data."""
    template_path = Path(__file__).parent / "index.html"
    
    with open(template_path) as f:
        html = f.read()
    
    # Replace the fetch call with embedded data
    data_json = json.dumps(data)
    
    # Replace the loadData function to use embedded data
    old_load = """async function loadData() {
            try {
                const resp = await fetch('data/export.json');
                const data = await resp.json();"""
    
    new_load = f"""async function loadData() {{
            try {{
                const data = {data_json};"""
    
    html = html.replace(old_load, new_load)
    
    with open(output_path, "w") as f:
        f.write(html)


if __name__ == "__main__":
    init_db()
    
    stats = get_stats()
    baseline = get_baseline_playoff_rate()
    players = export_all_player_odds(min_roster_pct=1.0)
    
    print(f"Exported {len(players)} players")
    print(f"Leagues: {stats['leagues']}, Rosters: {stats['rosters']}")
    print(f"Baseline playoff rate: {baseline}%")
    
    # Save to JSON
    output = {
        "stats": stats,
        "baseline_rate": baseline,
        "players": players,
    }
    
    output_path = Path(__file__).parent / "data" / "export.json"
    with open(output_path, "w") as f:
        json.dump(output, f)
    print(f"Saved JSON to {output_path}")
    
    # Generate standalone HTML
    html_path = Path(__file__).parent / "playoff_odds.html"
    generate_standalone_html(html_path, output)
    print(f"Saved standalone HTML to {html_path}")

