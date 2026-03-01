import sqlite3
import os
from typing import List, Dict, Tuple, Optional

# Check if PostgreSQL is being used
USE_POSTGRES = os.environ.get('DATABASE_URL') is not None

if USE_POSTGRES:
    import psycopg2
    from psycopg2 import extras

def get_db_connection():
    """Get a database connection - PostgreSQL or SQLite."""
    if USE_POSTGRES:
        conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
        # Set row factory for dict-like access
        extras.register_default_json(loads=lambda x: x)
        return conn
    else:
        conn = sqlite3.connect('matches.db', timeout=30)
        conn.row_factory = sqlite3.Row
        # Enable WAL mode for better concurrency
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        return conn

def execute_query(conn, query, params=(), fetch=False):
    """Execute a query with params, handling differences between SQLite and PostgreSQL."""
    if USE_POSTGRES:
        # Convert ? to %s for PostgreSQL
        pg_query = query.replace('?', '%s')
        cursor = conn.cursor()
        cursor.execute(pg_query, params)
        if fetch:
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        return cursor
    else:
        cursor = conn.execute(query, params)
        if fetch:
            return [dict(row) for row in cursor.fetchall()]
        return cursor

def init_db():
    """Initialize the database with the updated schema."""
    conn = get_db_connection()
    
    if USE_POSTGRES:
        cursor = conn.cursor()
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'matches'
            )
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            cursor.execute('''
                CREATE TABLE matches (
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
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(description, map, player, match_id)
                )
            ''')
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_player ON matches(player)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_description ON matches(description)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_match_date ON matches(match_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_match_id ON matches(match_id)')
            conn.commit()
            print("Created new matches table with full schema (PostgreSQL)")
        else:
            print("Database already up to date (PostgreSQL)")
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
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(description, map, player, match_id)
                )
            ''')
            print("Created new matches table with full schema (SQLite)")
        elif 'match_date' not in columns:
            conn.execute('ALTER TABLE matches ADD COLUMN match_date TEXT')
            conn.execute('ALTER TABLE matches ADD COLUMN result TEXT')
            conn.execute('ALTER TABLE matches ADD COLUMN team TEXT')
            conn.execute('ALTER TABLE matches ADD COLUMN tournament_id INTEGER')
            conn.execute('ALTER TABLE matches ADD COLUMN match_id TEXT')
            conn.execute('ALTER TABLE matches ADD COLUMN created_at TEXT DEFAULT CURRENT_TIMESTAMP')
            print("Migrated database to new schema (SQLite)")
        else:
            print("Database already up to date (SQLite)")
        
        # Create indexes
        conn.execute('CREATE INDEX IF NOT EXISTS idx_player ON matches(player)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_description ON matches(description)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_match_date ON matches(match_date)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_match_id ON matches(match_id)')
        conn.commit()
    
    conn.close()

def match_exists(description: str, map_name: str, player: str, match_id: str = None) -> bool:
    """Check if a match record already exists."""
    conn = get_db_connection()
    cursor = conn.cursor() if USE_POSTGRES else conn
    query = 'SELECT 1 FROM matches WHERE description = ? AND map = ? AND player = ? AND match_id = ? LIMIT 1'
    
    if USE_POSTGRES:
        cursor.execute(query.replace('?', '%s'), (description, map_name, player, match_id))
    else:
        cursor.execute(query, (description, map_name, player, match_id))
    
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
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO matches (description, map, player, kills, deaths, match_date, result, team, tournament_id, match_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (description, map, player, match_id) DO NOTHING
            ''', (description, map_name, player, kills, deaths, match_date, result, team, tournament_id, match_id))
        else:
            conn.execute('''
                INSERT OR IGNORE INTO matches (description, map, player, kills, deaths, match_date, result, team, tournament_id, match_id)
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
    Add multiple match records in a batch using bulk INSERT.
    Uses INSERT OR IGNORE (SQLite) or ON CONFLICT DO NOTHING (PostgreSQL).
    SINGLE QUERY instead of thousands.
    
    Returns:
        Tuple of (inserted_count, skipped_count)
    """
    if not matches:
        return 0, 0
    
    conn = get_db_connection()
    
    # Get current count before insert
    if USE_POSTGRES:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM matches')
        count_before = cursor.fetchone()[0]
    else:
        count_before = conn.execute('SELECT COUNT(*) FROM matches').fetchone()[0]
    
    # Prepare values
    values = [
        (
            m['description'],
            m['map'],
            m['player'],
            m['kills'],
            m['deaths'],
            m.get('match_date'),
            m.get('result'),
            m.get('team'),
            m.get('tournament_id'),
            m.get('match_id')
        )
        for m in matches
    ]
    
    if USE_POSTGRES:
        # PostgreSQL: use executemany with ON CONFLICT
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT INTO matches (description, map, player, kills, deaths, match_date, result, team, tournament_id, match_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (description, map, player, match_id) DO NOTHING
        ''', values)
    else:
        # SQLite: use executemany with INSERT OR IGNORE
        conn.executemany('''
            INSERT OR IGNORE INTO matches (description, map, player, kills, deaths, match_date, result, team, tournament_id, match_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', values)
    
    conn.commit()
    
    # Get count after insert to calculate inserted
    if USE_POSTGRES:
        cursor.execute('SELECT COUNT(*) FROM matches')
        count_after = cursor.fetchone()[0]
    else:
        count_after = conn.execute('SELECT COUNT(*) FROM matches').fetchone()[0]
    
    inserted = count_after - count_before
    skipped = len(matches) - inserted
    
    conn.close()
    return inserted, skipped

def get_scores(player: str = None, tournament: str = None):
    """Get aggregated scores with optional filtering."""
    conn = get_db_connection()
    query = '''
        SELECT kills, deaths, player, map, team, result, match_date, description
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
    
    if USE_POSTGRES:
        cursor = conn.cursor()
        cursor.execute(query.replace('?', '%s'), params)
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    else:
        rows = [dict(row) for row in conn.execute(query, params).fetchall()]
    
    conn.close()
    
    # Group by kills, deaths
    grouped = {}
    for row in rows:
        key = (row['kills'], row['deaths'])
        if key not in grouped:
            grouped[key] = {'count': 0, 'matches': []}
        grouped[key]['count'] += 1
        
        formatted = f"{row['player']} on {row['map']}"
        if row['team']:
            formatted += f"\n  Team: {row['team']}"
        if row['result']:
            formatted += f" | {row['result']}"
        if row['match_date']:
            formatted += f" | {row['match_date']}"
        if row['description']:
            formatted += f"\n  {row['description']}"
        
        if formatted not in grouped[key]['matches']:
            grouped[key]['matches'].append(formatted)
    
    result = {}
    for key, data in grouped.items():
        result[key] = {
            'count': data['count'],
            'details': '\n\n'.join(data['matches'])
        }
    
    return result

def get_total_matches() -> int:
    """Get total number of match records."""
    conn = get_db_connection()
    if USE_POSTGRES:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM matches')
        count = cursor.fetchone()[0]
    else:
        count = conn.execute('SELECT COUNT(*) FROM matches').fetchone()[0]
    conn.close()
    return count

def get_recent_matches(limit: int = 10) -> List[Dict]:
    """Get most recent matches."""
    conn = get_db_connection()
    query = 'SELECT * FROM matches ORDER BY created_at DESC LIMIT ?'
    
    if USE_POSTGRES:
        cursor = conn.cursor()
        cursor.execute(query.replace('?', '%s'), (limit,))
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    else:
        rows = [dict(row) for row in conn.execute(query, (limit,)).fetchall()]
    
    conn.close()
    return rows

def get_unique_players_list() -> List[str]:
    """Get list of all unique players."""
    conn = get_db_connection()
    if USE_POSTGRES:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT player FROM matches ORDER BY LOWER(player)')
        rows = [row[0] for row in cursor.fetchall()]
    else:
        rows = [row['player'] for row in conn.execute('SELECT DISTINCT player FROM matches ORDER BY LOWER(player)').fetchall()]
    conn.close()
    return rows

def get_unique_tournaments_list() -> List[str]:
    """Get list of all unique tournament descriptions."""
    conn = get_db_connection()
    if USE_POSTGRES:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT description FROM matches ORDER BY description')
        rows = [row[0] for row in cursor.fetchall()]
    else:
        rows = [row['description'] for row in conn.execute('SELECT DISTINCT description FROM matches ORDER BY description').fetchall()]
    conn.close()
    return rows

def verify_kill_death_balance() -> int:
    """Verify that total kills equals total deaths."""
    conn = get_db_connection()
    if USE_POSTGRES:
        cursor = conn.cursor()
        cursor.execute('SELECT SUM(kills) - SUM(deaths) as diff FROM matches')
        result = cursor.fetchone()[0]
    else:
        result = conn.execute('SELECT SUM(kills) - SUM(deaths) as diff FROM matches').fetchone()['diff']
    conn.close()
    return result if result else 0

def get_database_stats() -> Dict:
    """Get database statistics."""
    conn = get_db_connection()
    
    if USE_POSTGRES:
        cursor = conn.cursor()
        stats = {}
        
        cursor.execute('SELECT COUNT(*) FROM matches')
        stats['total_matches'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT player) FROM matches')
        stats['unique_players'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT map) FROM matches')
        stats['unique_maps'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT description) FROM matches')
        stats['unique_tournaments'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT SUM(kills) FROM matches')
        result = cursor.fetchone()[0]
        stats['total_kills'] = result if result else 0
        
        cursor.execute('SELECT SUM(deaths) FROM matches')
        result = cursor.fetchone()[0]
        stats['total_deaths'] = result if result else 0
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
    print(f"\nUsing: {'PostgreSQL' if USE_POSTGRES else 'SQLite'}")
    print("\nDatabase Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")