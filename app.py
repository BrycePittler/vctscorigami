from flask import Flask, render_template, request, redirect, url_for, flash
import database
import sqlite3
import bcrypt
import os

app = Flask(__name__)
app.secret_key = os.urandom(24)  # Required for flash messages

# Define the password (use an environment variable in production)
# For this example, we'll hash a sample password: "mysecretpassword"
# You can generate a new hashed password by running:
# python -c "import bcrypt; print(bcrypt.hashpw('string12string12'.encode('utf-8'), bcrypt.gensalt()))"
PASSWORD_HASH = b"$2b$12$UxfOKV7MadrhIWPhy1Sozu3r0fhwr8pgshjd9t08XhSEvA791fWZO"  # Replace with your hashed password

# Initialize database on startup
database.init_db()

@app.route('/')
def index():
    selected_player = request.args.get('player', 'all')
    selected_tournament = request.args.get('tournament', 'all')
    # Get aggregated scores, filtered by player and/or tournament
    scores = database.get_scores(
        selected_player if selected_player != 'all' else None,
        selected_tournament if selected_tournament != 'all' else None
    )
    # Find the maximum count for gradient scaling
    max_count = max([info['count'] for info in scores.values()]) if scores else 1
    # Get unique players for dropdown
    conn = database.get_db_connection()
    unique_players = conn.execute('SELECT DISTINCT player FROM matches ORDER BY player').fetchall()
    # Get unique tournaments
    unique_tournaments = database.get_unique_tournaments()
    # Get leaderboard: top 5 players by unique (kills, deaths) combos
    leaderboard_unique = conn.execute('''
        SELECT player, COUNT(DISTINCT kills || '-' || deaths) as unique_scores
        FROM matches
        GROUP BY player
        ORDER BY unique_scores DESC
        LIMIT 5
    ''').fetchall()
    # Get leaderboard: top 5 players by exclusive scorigami
    leaderboard_exclusive = conn.execute('''
        SELECT player, COUNT(DISTINCT kills || '-' || deaths) as exclusive_scores
        FROM matches m1
        WHERE NOT EXISTS (
            SELECT 1 FROM matches m2 
            WHERE m2.kills = m1.kills AND m2.deaths = m1.deaths AND m2.player != m1.player
        )
        GROUP BY player
        ORDER BY exclusive_scores DESC
        LIMIT 5
    ''').fetchall()
    # Get leaderboard: top 5 players by total maps played (total matches)
    leaderboard_maps_played = conn.execute('''
        SELECT player, COUNT(*) as total_matches
        FROM matches
        GROUP BY player
        ORDER BY total_matches DESC
        LIMIT 5
    ''').fetchall()
    # Get leaderboard: top 5 players by scorigami per match
    leaderboard_scorigami_per_match = conn.execute('''
        WITH player_stats AS (
            SELECT player, COUNT(*) as total_matches 
            FROM matches 
            GROUP BY player
        ),
        exclusive_combos AS (
            SELECT player, COUNT(DISTINCT kills || '-' || deaths) as exclusive_unique
            FROM matches m1
            WHERE NOT EXISTS (
                SELECT 1 FROM matches m2 
                WHERE m2.kills = m1.kills AND m2.deaths = m1.deaths AND m2.player != m1.player
            )
            GROUP BY player
        )
        SELECT ps.player, 
               COALESCE(CAST(ec.exclusive_unique AS FLOAT) / ps.total_matches, 0.0) as scorigami_per_match
        FROM player_stats ps
        LEFT JOIN exclusive_combos ec ON ps.player = ec.player
        ORDER BY scorigami_per_match DESC
        LIMIT 5
    ''').fetchall()
    conn.close()
    # Pass to HTML template
    return render_template('index.html', scores=scores, max_count=max_count,
                           leaderboard_unique=leaderboard_unique, 
                           leaderboard_exclusive=leaderboard_exclusive,
                           leaderboard_maps_played=leaderboard_maps_played,
                           leaderboard_scorigami_per_match=leaderboard_scorigami_per_match,
                           unique_players=unique_players, 
                           unique_tournaments=unique_tournaments,
                           selected_player=selected_player, 
                           selected_tournament=selected_tournament)

@app.route('/update', methods=['GET', 'POST'])
def update():
    # Check for password on both GET and POST
    password = request.form.get('password') if request.method == 'POST' else request.args.get('password')
    
    if not password or not bcrypt.checkpw(password.encode('utf-8'), PASSWORD_HASH):
        flash('Incorrect password. Access denied.')
        return redirect(url_for('index'))
    
    if request.method == 'POST':
        # Handle form submission from tile click
        tournament = request.form.get('tournament')
        stage = request.form.get('stage')
        match_type = request.form.get('match_type')
        match_name = request.form.get('match_name')
        map_name = request.form.get('map')
        player = request.form.get('player')
        kills = int(request.form.get('kills'))
        deaths = int(request.form.get('deaths'))
        # Combine fields into description
        description = f"{tournament} {stage} {match_type} {match_name}"
        # Validate kills and deaths
        if 0 <= kills <= 50 and 0 <= deaths <= 50:
            database.add_match(description, map_name, player, kills, deaths)
            flash('Match added successfully!')
        else:
            flash('Invalid kills or deaths. Must be between 0 and 50.')
        # Pass form values back to persist them
        return render_template('update.html',
                               scores=database.get_scores(),
                               max_count=max([info['count'] for info in database.get_scores().values()]) if database.get_scores() else 1,
                               tournament=tournament,
                               stage=stage,
                               match_type=match_type,
                               match_name=match_name,
                               map_name=map_name)
    # GET: Render the update page with the graph
    scores = database.get_scores()
    max_count = max([info['count'] for info in scores.values()]) if scores else 1
    return render_template('update.html', scores=scores, max_count=max_count,
                           tournament='', stage='', match_type='', match_name='', map_name='')

if __name__ == '__main__':
    app.run(debug=True)