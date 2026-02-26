"""
Initialize the Twitter bot tracking database.
Marks all existing scorigamis as 'already posted' so they won't be tweeted.
"""
import database

def init_posted_scorigamis():
    """Mark all existing scorigamis as already posted."""
    conn = database.get_db_connection()
    
    # Create the tracking table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS posted_scorigamis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kills INTEGER NOT NULL,
            deaths INTEGER NOT NULL,
            tweet_id TEXT,
            posted_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(kills, deaths)
        )
    ''')
    conn.commit()
    
    # Get all current scorigamis
    cursor = conn.execute('''
        SELECT kills, deaths
        FROM matches
        GROUP BY kills, deaths
        HAVING COUNT(*) = 1
    ''')
    scorigamis = cursor.fetchall()
    
    # Mark them all as posted (without tweet_id since we didn't actually tweet them)
    count = 0
    for row in scorigamis:
        try:
            conn.execute('''
                INSERT OR IGNORE INTO posted_scorigamis (kills, deaths, tweet_id)
                VALUES (?, ?, NULL)
            ''', (row['kills'], row['deaths']))
            count += 1
        except Exception as e:
            print(f"Error: {e}")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Marked {count} existing scorigamis as 'already posted'")
    print("   The bot will only tweet NEW scorigamis going forward.")

if __name__ == '__main__':
    database.init_db()
    init_posted_scorigamis()