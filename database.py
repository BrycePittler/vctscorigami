import sqlite3
import os
from typing import List, Dict, Tuple, Optional

# Check if PostgreSQL is being used
USE_POSTGRES = False
try:
    if os.environ.get('DATABASE_URL'):
        import psycopg2
        from psycopg2 import extras
        USE_POSTGRES = True
except ImportError:
    pass

def get_db_connection():
    """Get a database connection - PostgreSQL or SQLite."""
    if USE_POSTGRES:
        conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
        return conn
    else:
        conn = sqlite3.connect('matches.db', timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        return conn

def fetchone(conn, query, params=()):
    """Execute query and fetch one result as dict."""
    if USE_POSTGRES:
        cursor = conn.cursor()
        pg_query = query.replace('?', '%s')
        cursor.execute(pg_query, params)
        row = cursor.fetchone()
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return None
    else:
        row = conn.execute(query, params).fetchone()
        return dict(row) if row else None

def fetchall(conn, query, params=()):
    """Execute query and fetch all results as list of dicts."""
    if USE_POSTGRES:
        cursor = conn.cursor()
        pg_query = query.replace('?', '%s')
        cursor.execute(pg_query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    else:
        return [dict(row) for row in conn.execute(query, params).fetchall()]

def init_db():
    """Initialize the database with the updated schema."""
    conn = get_db_connection()
    
    if USE_POSTGRES:
        cursor = conn.cursor()
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
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_player ON matches(player)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_description ON matches(description)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_match_date ON matches(match_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_match_id ON matches(match_id)')
            conn.commit()
            print("Created new matches table (PostgreSQL)")
        else:
            print("Database already up to date (PostgreSQL)")
    else:
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
            print("Created new matches table (SQLite)")
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
        
        conn.execute('CREATE INDEX IF NOT EXISTS idx_player ON matches(player)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_description ON matches(description)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_match_date ON matches(match_date)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_match_id ON matches(match_id)')
        conn.commit()
    
    conn.close()

def add_matches_batch(matches: List[Dict]) -> Tuple[int, int]:
    """Bulk insert with ON CONFLICT - single query."""
    if not matches:
        return 0, 0
    
    conn = get_db_connection()
    
    row = fetchone(conn, 'SELECT COUNT(*) as count FROM matches')
    count_before = row['count']
    
    values = [
        (
            m['description'], m['map'], m['player'], m['kills'], m['deaths'],
            m.get('match_date'), m.get('result'), m.get('team'),
            m.get('tournament_id'), m.get('match_id')
        )
        for m in matches
    ]
    
    if USE_POSTGRES:
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT INTO matches (description, map, player, kills, deaths, match_date, result, team, tournament_id, match_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (description, map, player, match_id) DO NOTHING
        ''', values)
    else:
        conn.executemany('''
            INSERT OR IGNORE INTO matches (description, map, player, kills, deaths, match_date, result, team, tournament_id, match_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', values)
    
    conn.commit()
    
    row = fetchone(conn, 'SELECT COUNT(*) as count FROM matches')
    count_after = row['count']
    
    inserted = count_after - count_before
    skipped = len(matches) - inserted
    
    conn.close()
    return inserted, skipped

def get_scores(player: str = None, tournament: str = None):
    """Get aggregated scores with optional filtering."""
    conn = get_db_connection()
    query = 'SELECT kills, deaths, player, map, team, result, match_date, description FROM matches'
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
    
    rows = fetchall(conn, query, params)
    conn.close()
    
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
        result[key] = {'count': data['count'], 'details': '\n\n'.join(data['matches'])}
    
    return result

def get_database_stats() -> Dict:
    """Get database statistics."""
    conn = get_db_connection()
    
    stats = {}
    for key, query in [
        ('total_matches', 'SELECT COUNT(*) as val FROM matches'),
        ('unique_players', 'SELECT COUNT(DISTINCT player) as val FROM matches'),
        ('unique_maps', 'SELECT COUNT(DISTINCT map) as val FROM matches'),
        ('unique_tournaments', 'SELECT COUNT(DISTINCT description) as val FROM matches'),
        ('total_kills', 'SELECT SUM(kills) as val FROM matches'),
        ('total_deaths', 'SELECT SUM(deaths) as val FROM matches'),
    ]:
        row = fetchone(conn, query)
        stats[key] = row['val'] if row['val'] else 0
    
    stats['kd_balance'] = stats['total_kills'] - stats['total_deaths']
    conn.close()
    return stats


if __name__ == '__main__':
    init_db()
    stats = get_database_stats()
    print(f"\nUsing: {'PostgreSQL' if USE_POSTGRES else 'SQLite'}")
    print("\nDatabase Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")