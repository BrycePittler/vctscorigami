import sqlite3

# Predefined list of all Valorant Masters and Champions tournaments (2021–2025)
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

# Connect to the database file (creates it if it doesn't exist)
def get_db_connection():
    conn = sqlite3.connect('matches.db')
    conn.row_factory = sqlite3.Row
    return conn

# Create the table if it doesn't exist
def init_db():
    conn = get_db_connection()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT NOT NULL,
            map TEXT NOT NULL,
            player TEXT NOT NULL,
            kills INTEGER NOT NULL,
            deaths INTEGER NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

# Add a new match
def add_match(description, map_name, player, kills, deaths):
    conn = get_db_connection()
    conn.execute(
        'INSERT INTO matches (description, map, player, kills, deaths) VALUES (?, ?, ?, ?, ?)',
        (description, map_name, player, kills, deaths)
    )
    conn.commit()
    conn.close()
    return True

# Get aggregated scores
def get_scores(player=None, tournament=None):
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

# Get unique tournaments
def get_unique_tournaments():
    return sorted(MASTERS_CHAMPIONS_TOURNAMENTS)

if __name__ == '__main__':
    init_db()