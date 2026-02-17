import sqlite3
from typing import List, Dict, Tuple, Optional
from datetime import datetime

def get_db_connection():
    """Get a database connection."""
    conn = sqlite3.connect('matches.db')
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize the database with the updated schema."""
    conn = get_db_connection()
    
    # Check if table exists
    cursor = conn.execute("PRAGMA table_info(matches)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if 'matches' not in [t[0] for t in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]:
        # Create new table with full schema
        conn.execute('''
            CREATE TABLE matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                description TEXT NOT NULL,
                map TEXT NOT NULL,
                player TEXT NOT NULL,
                kills INTEGER NOT NULL,
                deaths INTEGER NOT NULL,
                match_date TEXT,
                result TEXT,
                team TEXT,
                tournament_id INTEGER,
                match_id TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("Created new matches table with full schema")
    elif 'match_date' not in columns:
        # Migrate old schema to new schema
        conn.execute('ALTER TABLE matches ADD COLUMN match_date TEXT')
        conn.execute('ALTER TABLE matches ADD COLUMN result TEXT')
        conn.execute('ALTER TABLE matches ADD COLUMN team TEXT')
        conn.execute('ALTER TABLE matches ADD COLUMN tournament_id INTEGER')
        conn.execute('ALTER TABLE matches ADD COLUMN match_id TEXT')
        conn.execute('ALTER TABLE matches ADD COLUMN created_at TEXT DEFAULT CURRENT_TIMESTAMP')
        print("Migrated database to new schema")
    else:
        print("Database already up to date")
    
    # Create indexes - UPDATED to include match_id for uniqueness
    conn.execute('CREATE INDEX IF NOT EXISTS idx_player ON matches(player)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_description ON matches(description)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_match_date ON matches(match_date)')
    # New unique index includes match_id to handle teams playing each other multiple times
    conn.execute('CREATE INDEX IF NOT EXISTS idx_unique_match ON matches(match_id, map, player)')
    
    conn.commit()
    conn.close()

def match_exists(match_id: str, map_name: str, player: str) -> bool:
    """Check if a match record already exists."""
    conn = get_db_connection()
    cursor = conn.execute(
        'SELECT 1 FROM matches WHERE match_id = ? AND map = ? AND player = ? LIMIT 1',
        (match_id, map_name, player)
    )
    exists = cursor.fetchone() is not None
    conn.close()
    return exists

def add_match(description: str, map_name: str, player: str, kills: int, deaths: int,
              match_date: str = None, result: str = None, team: str = None,
              tournament_id: int = None, match_id: str = None) -> bool:
    """Add a single match record."""
    conn = get_db_connection()
    try:
        conn.execute('''
            INSERT INTO matches (description, map, player, kills, deaths, match_date, result, team, tournament_id, match_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (description, map_name, player, kills, deaths, match_date, result, team, tournament_id, match_id))
        conn.commit()
        return True
    except Exception as e:
        print(f"Error adding match: {e}")
        return False
    finally:
        conn.close()

def add_matches_batch(matches: List[Dict]) -> Tuple[int, int]:
    """
    Add multiple match records in a batch.
    Skips duplicates based on (match_id, map, player).
    
    Returns:
        Tuple of (inserted_count, skipped_count)
    """
    conn = get_db_connection()
    inserted = 0
    skipped = 0
    
    for match in matches:
        match_id = match.get('match_id')
        if not match_id:
            # Fallback to old method if no match_id
            cursor = conn.execute(
                'SELECT 1 FROM matches WHERE description = ? AND map = ? AND player = ? LIMIT 1',
                (match['description'], match['map'], match['player'])
            )
        else:
            cursor = conn.execute(
                'SELECT 1 FROM matches WHERE match_id = ? AND map = ? AND player = ? LIMIT 1',
                (match_id, match['map'], match['player'])
            )
        
        if cursor.fetchone():
            skipped += 1
            continue
        
        try:
            conn.execute('''
                INSERT INTO matches (description, map, player, kills, deaths, match_date, result, team, tournament_id, match_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                match['description'],
                match['map'],
                match['player'],
                match['kills'],
                match['deaths'],
                match.get('match_date'),
                match.get('result'),
                match.get('team'),
                match.get('tournament_id'),
                match.get('match_id')
            ))
            inserted += 1
        except Exception as e:
            print(f"Error inserting match: {e}")
            skipped += 1
    
    conn.commit()
    conn.close()
    return inserted, skipped

def get_scores(player: str = None, tournament: str = None):
    """Get aggregated scores with optional filtering."""
    conn = get_db_connection()
    query = '''
        SELECT kills, deaths, COUNT(*) as count,
        GROUP_CONCAT(description || ' | Map: ' || map || ' | Player: ' || player, '\n') as details
        FROM matches
    '''
    params = []
    conditions = []
    if player:
        conditions.append('player = ?')
        params.append(player)
    if tournament:
        conditions.append("description LIKE ? || '%'")
        params.append(tournament)
    if conditions:
        query += ' WHERE ' + ' AND '.join(conditions)
    query += ' GROUP BY kills, deaths'
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return {(row['kills'], row['deaths']): {'count': row['count'], 'details': row['details']} for row in rows}

def get_total_matches() -> int:
    """Get total number of match records."""
    conn = get_db_connection()
    count = conn.execute('SELECT COUNT(*) FROM matches').fetchone()[0]
    conn.close()
    return count

def get_recent_matches(limit: int = 10) -> List[Dict]:
    """Get most recent matches."""
    conn = get_db_connection()
    rows = conn.execute('''
        SELECT * FROM matches 
        ORDER BY created_at DESC 
        LIMIT ?
    ''', (limit,)).fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_unique_players_list() -> List[str]:
    """Get list of all unique players."""
    conn = get_db_connection()
    rows = conn.execute('SELECT DISTINCT player FROM matches ORDER BY LOWER(player)').fetchall()
    conn.close()
    return [row['player'] for row in rows]

def get_unique_tournaments_list() -> List[str]:
    """Get list of all unique tournament descriptions."""
    conn = get_db_connection()
    rows = conn.execute('SELECT DISTINCT description FROM matches ORDER BY description').fetchall()
    conn.close()
    return [row['description'] for row in rows]

def verify_kill_death_balance() -> int:
    """
    Verify that total kills equals total deaths.
    Returns the difference (should be 0 for valid data).
    """
    conn = get_db_connection()
    result = conn.execute('SELECT SUM(kills) - SUM(deaths) as diff FROM matches').fetchone()
    conn.close()
    return result['diff'] if result else 0

def get_database_stats() -> Dict:
    """Get database statistics."""
    conn = get_db_connection()
    stats = {
        'total_matches': conn.execute('SELECT COUNT(*) FROM matches').fetchone()[0],
        'unique_players': conn.execute('SELECT COUNT(DISTINCT player) FROM matches').fetchone()[0],
        'unique_maps': conn.execute('SELECT COUNT(DISTINCT map) FROM matches').fetchone()[0],
        'unique_tournaments': conn.execute('SELECT COUNT(DISTINCT description) FROM matches').fetchone()[0],
        'total_kills': conn.execute('SELECT SUM(kills) FROM matches').fetchone()[0] or 0,
        'total_deaths': conn.execute('SELECT SUM(deaths) FROM matches').fetchone()[0] or 0,
    }
    stats['kd_balance'] = stats['total_kills'] - stats['total_deaths']
    conn.close()
    return stats


# Predefined list of all Valorant Masters and Champions tournaments
MASTERS_CHAMPIONS_TOURNAMENTS = [
    "Champions Tour 2023: Lock-In Sao Paulo",
    "Champions Tour 2023: Masters Tokyo",
    "Valorant Champions 2023",
    "Champions Tour 2024: Masters Madrid",
    "Champions Tour 2024: Masters Shanghai",
    "Valorant Champions 2024",
    "VCT 2025: Masters Bangkok",
    "VCT 2025: Masters Toronto",
    "Valorant Champions 2025"
]

def get_unique_tournaments():
    """Get unique tournaments - for backwards compatibility."""
    return sorted(MASTERS_CHAMPIONS_TOURNAMENTS)


if __name__ == '__main__':
    init_db()
    stats = get_database_stats()
    print("\nDatabase Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")