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
    
    def execute_query(conn, query, params=None):
        """Execute a query with cursor (PostgreSQL)."""
        cur = conn.cursor()
        cur.execute(query, params or ())
        return cur
    
    def execute_dict_query(conn, query, params=None):
        """Execute a query and return dict results (PostgreSQL)."""
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query, params or ())
        return cur
else:
    def get_db_connection():
        """Get SQLite connection."""
        conn = sqlite3.connect('matches.db')
        conn.row_factory = sqlite3.Row
        return conn
    
    def execute_query(conn, query, params=None):
        """Execute a query (SQLite)."""
        return conn.execute(query, params or ())
    
    def execute_dict_query(conn, query, params=None):
        """Execute a query and return dict results (SQLite)."""
        return conn.execute(query, params or ())


def init_db():
    """Initialize the database with the updated schema."""
    conn = get_db_connection()
    
    if USE_POSTGRES:
        # PostgreSQL
        execute_query(conn, '''
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
        execute_query(conn, 'CREATE INDEX IF NOT EXISTS idx_player ON matches(player)')
        execute_query(conn, 'CREATE INDEX IF NOT EXISTS idx_description ON matches(description)')
        execute_query(conn, 'CREATE INDEX IF NOT EXISTS idx_match_date ON matches(match_date)')
        execute_query(conn, 'CREATE INDEX IF NOT EXISTS idx_unique_match ON matches(match_id, map, player)')
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


def add_matches_batch(matches: List[Dict]) -> Tuple[int, int]:
    """Add multiple match records in a batch."""
    conn = get_db_connection()
    inserted = 0
    skipped = 0
    
    for match in matches:
        match_id = match.get('match_id')
        
        if match_id:
            cur = execute_query(conn, 
                'SELECT 1 FROM matches WHERE match_id = %s AND map = %s AND player = %s LIMIT 1' if USE_POSTGRES else
                'SELECT 1 FROM matches WHERE match_id = ? AND map = ? AND player = ? LIMIT 1',
                (match_id, match['map'], match['player'])
            )
        else:
            cur = execute_query(conn,
                'SELECT 1 FROM matches WHERE description = %s AND map = %s AND player = %s LIMIT 1' if USE_POSTGRES else
                'SELECT 1 FROM matches WHERE description = ? AND map = ? AND player = ? LIMIT 1',
                (match['description'], match['map'], match['player'])
            )
        
        if cur.fetchone():
            skipped += 1
            if USE_POSTGRES:
                cur.close()
            continue
        
        try:
            execute_query(conn,
                'INSERT INTO matches (description, map, player, kills, deaths, match_date, result, team, tournament_id, match_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' if USE_POSTGRES else
                'INSERT INTO matches (description, map, player, kills, deaths, match_date, result, team, tournament_id, match_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (
                    match['description'], match['map'], match['player'],
                    match['kills'], match['deaths'],
                    match.get('match_date'), match.get('result'), match.get('team'),
                    match.get('tournament_id'), match.get('match_id')
                )
            )
            if not USE_POSTGRES:
                conn.commit()
            inserted += 1
        except Exception as e:
            print(f"Error inserting match: {e}")
            skipped += 1
        
        if USE_POSTGRES:
            cur.close()
    
    conn.close()
    return inserted, skipped


def get_scores(player: str = None, tournament: str = None):
    """Get aggregated scores with optional filtering."""
    conn = get_db_connection()
    query = 'SELECT kills, deaths, COUNT(*) as count FROM matches'
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
    
    cur = execute_query(conn, query, params) if params else execute_query(conn, query)
    rows = cur.fetchall()
    
    result = {}
    for row in rows:
        if USE_POSTGRES:
            result[(row[0], row[1])] = {'count': row[2]}
        else:
            result[(row['kills'], row['deaths'])] = {'count': row['count']}
    
    if USE_POSTGRES:
        cur.close()
    conn.close()
    return result


def get_database_stats() -> Dict:
    """Get database statistics."""
    conn = get_db_connection()
    
    cur = execute_query(conn, 'SELECT COUNT(*) FROM matches')
    total_matches = cur.fetchone()[0]
    if USE_POSTGRES:
        cur.close()
    
    cur = execute_query(conn, 'SELECT COUNT(DISTINCT player) FROM matches')
    unique_players = cur.fetchone()[0]
    if USE_POSTGRES:
        cur.close()
    
    cur = execute_query(conn, 'SELECT COUNT(DISTINCT map) FROM matches')
    unique_maps = cur.fetchone()[0]
    if USE_POSTGRES:
        cur.close()
    
    cur = execute_query(conn, 'SELECT COUNT(DISTINCT description) FROM matches')
    unique_tournaments = cur.fetchone()[0]
    if USE_POSTGRES:
        cur.close()
    
    cur = execute_query(conn, 'SELECT SUM(kills) FROM matches')
    total_kills = cur.fetchone()[0] or 0
    if USE_POSTGRES:
        cur.close()
    
    cur = execute_query(conn, 'SELECT SUM(deaths) FROM matches')
    total_deaths = cur.fetchone()[0] or 0
    if USE_POSTGRES:
        cur.close()
    
    conn.close()
    
    return {
        'total_matches': total_matches,
        'unique_players': unique_players,
        'unique_maps': unique_maps,
        'unique_tournaments': unique_tournaments,
        'total_kills': total_kills,
        'total_deaths': total_deaths,
        'kd_balance': total_kills - total_deaths
    }


def verify_kill_death_balance() -> int:
    """Verify that total kills equals total deaths."""
    conn = get_db_connection()
    cur = execute_query(conn, 'SELECT SUM(kills) - SUM(deaths) FROM matches')
    result = cur.fetchone()[0]
    if USE_POSTGRES:
        cur.close()
    conn.close()
    return result if result else 0


def get_unique_players_list() -> List[str]:
    """Get list of all unique players."""
    conn = get_db_connection()
    cur = execute_query(conn, 'SELECT DISTINCT player FROM matches ORDER BY LOWER(player)')
    rows = cur.fetchall()
    if USE_POSTGRES:
        cur.close()
    conn.close()
    return [row[0] for row in rows]


def get_unique_tournaments_list() -> List[str]:
    """Get list of all unique tournament descriptions."""
    conn = get_db_connection()
    cur = execute_query(conn, 'SELECT DISTINCT description FROM matches ORDER BY description')
    rows = cur.fetchall()
    if USE_POSTGRES:
        cur.close()
    conn.close()
    return [row[0] for row in rows]


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