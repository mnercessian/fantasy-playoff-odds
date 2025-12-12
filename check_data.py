from db import get_connection

def check_monangai():
    conn = get_connection()
    cursor = conn.cursor()

    # Find player
    print("Searching for Monangai...")
    cursor.execute("SELECT player_id, full_name, team, position FROM players WHERE full_name LIKE '%Monangai%'")
    players = cursor.fetchall()
    print(f'Found players: {players}')

    if players:
        for p in players:
            player_id = p['player_id']
            print(f"\nChecking stats for {p['full_name']} ({player_id}):")
            
            # Get counts for this player
            cursor.execute("SELECT COUNT(*) FROM roster_players WHERE player_id = ?", (player_id,))
            player_count = cursor.fetchone()[0]
            
            # Get total rosters count
            cursor.execute("SELECT COUNT(*) FROM rosters WHERE made_playoffs IS NOT NULL")
            total_rosters = cursor.fetchone()[0]
            
            print(f'Player Count: {player_count}')
            print(f'Total Rosters: {total_rosters}')
            if total_rosters > 0:
                print(f'Calculated %: {player_count / total_rosters * 100:.2f}%')
            
            # Check a few leagues
            cursor.execute("""
                SELECT l.league_id, l.season
                FROM leagues l
                JOIN rosters r ON l.league_id = r.league_id
                JOIN roster_players rp ON r.roster_id = rp.roster_id AND r.league_id = rp.league_id
                WHERE rp.player_id = ?
                LIMIT 5
            """, (player_id,))
            leagues = cursor.fetchall()
            print(f'Sample Leagues: {leagues}')

    conn.close()

if __name__ == "__main__":
    check_monangai()

