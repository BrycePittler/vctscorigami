import os
import sqlite3
from typing import List, Dict, Tuple, Optional

# Check if we're using PostgreSQL (on Render) or SQLite (local)
DATABASE_URL = os.environ.get('DATABASE_URL')
USE_POSTGRES = DATABASE_URL is not None

if USE_POSTGRES:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    
    def get_db_connection():
        """Get PostgreSQL connection."""
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
        return conn
else:
    def get_db_connection():
        """Get SQLite connection."""
        conn = sqlite3.connect('matches.db')
        conn.row_factory = sqlite3.Row
        return conn


def init_db():
    """Initialize the database with the updated schema."""
    conn = get_db_connection()
    
    if USE_POSTGRES:
        # PostgreSQL
        conn.execute('''
            CREATE TABLE IF NOT EXISTS matches (
                id SERIAL PRIMARY KEY,
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Create indexes
        conn.execute('CREATE INDEX IF NOT EXISTS idx_player ON matches(player)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_description ON matches(description)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_match_date ON matches(match_date)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_unique_match ON matches(match_id, map, player)')
    else:
        # SQLite
        cursor = conn.execute("PRAGMA table_info(matches)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'matches' not in [t[0] for t in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]:
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
            print("Created new matches table")
        elif 'match_date' not in columns:
            conn.execute('ALTER TABLE matches ADD COLUMN match_date TEXT')
            conn.execute('ALTER TABLE matches ADD COLUMN result TEXT')
            conn.execute('ALTER TABLE matches ADD COLUMN team TEXT')
            conn.execute('ALTER TABLE matches ADD COLUMN tournament_id INTEGER')
            conn.execute('ALTER TABLE matches ADD COLUMN match_id TEXT')
            conn.execute('ALTER TABLE matches ADD COLUMN created_at TEXT DEFAULT CURRENT_TIMESTAMP')
            print("Migrated database")
        
        conn.execute('CREATE INDEX IF NOT EXISTS idx_player ON matches(player)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_description ON matches(description)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_match_date ON matches(match_date)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_unique_match ON matches(match_id, map, player)')
        conn.commit()
    
    conn.close()
    print("Database initialized")


def match_exists(match_id: str, map_name: str, player: str) -> bool:
    """Check if a match record already exists."""
    conn = get_db_connection()
    if match_id:
        cursor = conn.execute(
            'SELECT 1 FROM matches WHERE match_id = %s AND map = %s AND player = %s LIMIT 1' if USE_POSTGRES else
            'SELECT 1 FROM matches WHERE match_id = ? AND map = ? AND player = ? LIMIT 1',
            (match_id, map_name, player)
        )
    else:
        cursor = conn.execute(
            'SELECT 1 FROM matches WHERE description = %s AND map = %s AND player = %s LIMIT 1' if USE_POSTGRES else
            'SELECT 1 FROM matches WHERE description = ? AND map = ? AND player = ? LIMIT 1',
            (description, map_name, player)
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
        if USE_POSTGRES:
            conn.execute('''
                INSERT INTO matches (description, map, player, kills, deaths, match_date, result, team, tournament_id, match_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (description, map_name, player, kills, deaths, match_date, result, team, tournament_id, match_id))
        else:
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
    """Add multiple match records in a batch."""
    conn = get_db_connection()
    inserted = 0
    skipped = 0
    
    for match in matches:
        match_id = match.get('match_id')
        
        if match_id:
            cursor = conn.execute(
                'SELECT 1 FROM matches WHERE match_id = %s AND map = %s AND player = %s LIMIT 1' if USE_POSTGRES else
                'SELECT 1 FROM matches WHERE match_id = ? AND map = ? AND player = ? LIMIT 1',
                (match_id, match['map'], match['player'])
            )
        else:
            cursor = conn.execute(
                'SELECT 1 FROM matches WHERE description = %s AND map = %s AND player = %s LIMIT 1' if USE_POSTGRES else
                'SELECT 1 FROM matches WHERE description = ? AND map = ? AND player = ? LIMIT 1',
                (match['description'], match['map'], match['player'])
            )
        
        if cursor.fetchone():
            skipped += 1
            continue
        
        try:
            if USE_POSTGRES:
                conn.execute('''
                    INSERT INTO matches (description, map, player, kills, deaths, match_date, result, team, tournament_id, match_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    match['description'], match['map'], match['player'],
                    match['kills'], match['deaths'],
                    match.get('match_date'), match.get('result'), match.get('team'),
                    match.get('tournament_id'), match.get('match_id')
                ))
            else:
                conn.execute('''
                    INSERT INTO matches (description, map, player, kills, deaths, match_date, result, team, tournament_id, match_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    match['description'], match['map'], match['player'],
                    match['kills'], match['deaths'],
                    match.get('match_date'), match.get('result'), match.get('team'),
                    match.get('tournament_id'), match.get('match_id')
                ))
                conn.commit()
            inserted += 1
        except Exception as e:
            print(f"Error inserting match: {e}")
            skipped += 1
    
    if not USE_POSTGRES:
        conn.commit()
    conn.close()
    return inserted, skipped


def get_scores(player: str = None, tournament: str = None):
    """Get aggregated scores with optional filtering."""
    conn = get_db_connection()
    query = '''
        SELECT kills, deaths, COUNT(*) as count
        FROM matches
    '''
    params = []
    conditions = []
    if player:
        conditions.append('player = %s' if USE_POSTGRES else 'player = ?')
        params.append(player)
    if tournament:
        conditions.append('description LIKE %s' if USE_POSTGRES else "description LIKE ?")
        params.append(f"{tournament}%")
    if conditions:
        query += ' WHERE ' + ' AND '.join(conditions)
    query += ' GROUP BY kills, deaths'
    
    cursor = conn.execute(query, params) if params else conn.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    if USE_POSTGRES:
        return {(row['kills'], row['deaths']): {'count': row['count']} for row in rows}
    else:
        return {(row['kills'], row['deaths']): {'count': row['count']} for row in rows}


def get_database_stats() -> Dict:
    """Get database statistics."""
    conn = get_db_connection()
    
    if USE_POSTGRES:
        stats = {
            'total_matches': conn.execute('SELECT COUNT(*) FROM matches').fetchone()[0],
            'unique_players': conn.execute('SELECT COUNT(DISTINCT player) FROM matches').fetchone()[0],
            'unique_maps': conn.execute('SELECT COUNT(DISTINCT map) FROM matches').fetchone()[0],
            'unique_tournaments': conn.execute('SELECT COUNT(DISTINCT description) FROM matches').fetchone()[0],
            'total_kills': conn.execute('SELECT SUM(kills) FROM matches').fetchone()[0] or 0,
            'total_deaths': conn.execute('SELECT SUM(deaths) FROM matches').fetchone()[0] or 0,
        }
    else:
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


def verify_kill_death_balance() -> int:
    """Verify that total kills equals total deaths."""
    conn = get_db_connection()
    result = conn.execute('SELECT SUM(kills) - SUM(deaths) as diff FROM matches').fetchone()
    conn.close()
    return result[0] if result else 0


def get_unique_players_list() -> List[str]:
    """Get list of all unique players."""
    conn = get_db_connection()
    rows = conn.execute('SELECT DISTINCT player FROM matches ORDER BY LOWER(player)').fetchall()
    conn.close()
    return [row[0] if USE_POSTGRES else row['player'] for row in rows]


def get_unique_tournaments_list() -> List[str]:
    """Get list of all unique tournament descriptions."""
    conn = get_db_connection()
    rows = conn.execute('SELECT DISTINCT description FROM matches ORDER BY description').fetchall()
    conn.close()
    return [row[0] if USE_POSTGRES else row['description'] for row in rows]


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
    return sorted(MASTERS_CHAMPIONS_TOURNAMENTS)


if __name__ == '__main__':
    init_db()
    stats = get_database_stats()
    print("\nDatabase Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")