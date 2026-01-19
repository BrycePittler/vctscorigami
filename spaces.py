import sqlite3

def clean_database():
    db_name = 'matches.db'
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    print("Cleaning leading/trailing spaces from database...")

    # LTRIM removes leading spaces, RTRIM removes trailing spaces
    # We apply it to 'player', 'map', and 'description' just to be safe
    cursor.execute("UPDATE matches SET player = TRIM(player)")
    cursor.execute("UPDATE matches SET map = TRIM(map)")
    cursor.execute("UPDATE matches SET description = TRIM(description)")

    conn.commit()
    changes = conn.total_changes
    conn.close()

    print(f"Done! Cleaned up {changes} rows.")

if __name__ == "__main__":
    clean_database()