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

def rank_leaderboard(leaderboard_data, score_key):
    """
    Assigns ranks to a leaderboard list, handling ties correctly.
    Players with the same score get the same rank.
    """
    if not leaderboard_data:
        return []
    
    ranked_list = []
    current_rank = 1
    
    # Sort the data in descending order of the score
    sorted_data = sorted(leaderboard_data, key=lambda x: x[score_key], reverse=True)
    
    if sorted_data:
        ranked_list.append({'rank': current_rank, **sorted_data[0]})
        
        for i in range(1, len(sorted_data)):
            # Check for a tie with the previous entry
            if sorted_data[i][score_key] == sorted_data[i-1][score_key]:
                ranked_list.append({'rank': current_rank, **sorted_data[i]})
            else:
                current_rank = i + 1
                ranked_list.append({'rank': current_rank, **sorted_data[i]})
                
    return ranked_list

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
    
    conn = database.get_db_connection()
    
    # Get a list of all scorigami scores (scores that have only one occurrence in the entire database)
    overall_scorigamis_raw = conn.execute('''
        SELECT kills, deaths
        FROM matches
        GROUP BY kills, deaths
        HAVING COUNT(*) = 1
    ''').fetchall()
    
    # Convert the list of tuples to a set for efficient lookup in the template
    overall_scorigamis = {(s['kills'], s['deaths']) for s in overall_scorigamis_raw}
    
    # Get unique players for dropdown, now sorted in a case-insensitive way
    unique_players = conn.execute('SELECT DISTINCT player FROM matches ORDER BY LOWER(player)').fetchall()
    
    # Get unique tournaments
    unique_tournaments = database.get_unique_tournaments()
    
    # Get all data for each leaderboard for proper ranking
    leaderboard_unique_raw = conn.execute('''
        SELECT player, COUNT(DISTINCT kills || '-' || deaths) as unique_scores
        FROM matches
        GROUP BY player
        ORDER BY unique_scores DESC
    ''').fetchall()
    
    leaderboard_exclusive_raw = conn.execute('''
        SELECT player, COUNT(DISTINCT kills || '-' || deaths) as exclusive_scores
        FROM matches m1
        WHERE NOT EXISTS (
            SELECT 1 FROM matches m2 
            WHERE m2.kills = m1.kills AND m2.deaths = m1.deaths AND m2.player != m1.player
        )
        GROUP BY player
        ORDER BY exclusive_scores DESC
    ''').fetchall()
    
    leaderboard_maps_played_raw = conn.execute('''
        SELECT player, COUNT(*) as total_matches
        FROM matches
        GROUP BY player
        ORDER BY total_matches DESC
    ''').fetchall()
    
    # Kills - Deaths leaderboard query
    leaderboard_kd_raw = conn.execute('''
        SELECT player, SUM(kills) - SUM(deaths) AS kill_death_difference
        FROM matches
        GROUP BY player
        ORDER BY kill_death_difference DESC
    ''').fetchall()

    # Get total kills and deaths for all players
    totals = conn.execute('SELECT SUM(kills) as total_kills, SUM(deaths) as total_deaths FROM matches').fetchone()
    total_kills = totals['total_kills'] if totals and totals['total_kills'] else 0
    total_deaths = totals['total_deaths'] if totals and totals['total_deaths'] else 0

    conn.close()
    
    # Rank the raw data
    leaderboard_unique = rank_leaderboard(leaderboard_unique_raw, 'unique_scores')
    leaderboard_exclusive = rank_leaderboard(leaderboard_exclusive_raw, 'exclusive_scores')
    leaderboard_maps_played = rank_leaderboard(leaderboard_maps_played_raw, 'total_matches')
    leaderboard_kd = rank_leaderboard(leaderboard_kd_raw, 'kill_death_difference')

    # Pass the new variable to the template
    return render_template('index.html', scores=scores, max_count=max_count,
                           leaderboard_unique=leaderboard_unique, 
                           leaderboard_exclusive=leaderboard_exclusive,
                           leaderboard_maps_played=leaderboard_maps_played,
                           leaderboard_kd=leaderboard_kd,
                           unique_players=unique_players, 
                           unique_tournaments=unique_tournaments,
                           selected_player=selected_player, 
                           selected_tournament=selected_tournament,
                           total_kills=total_kills,
                           total_deaths=total_deaths,
                           overall_scorigamis=overall_scorigamis)

@app.route('/update', methods=['GET', 'POST'])
def update():
    from flask import session  # Import session here to avoid modifying imports
    if request.method == 'POST':
        # Check if already authenticated or validate password
        if 'authenticated' not in session:
            password = request.form.get('password')
            if not password or not bcrypt.checkpw(password.encode('utf-8'), PASSWORD_HASH):
                flash('Incorrect password. Access denied.')
                return redirect(url_for('index'))
            session['authenticated'] = True  # Mark session as authenticated

        # Process form data
        tournament = request.form.get('tournament')
        stage = request.form.get('stage')
        match_type = request.form.get('match_type')
        match_name = request.form.get('match_name')
        map_name = request.form.get('map')
        player = request.form.get('player')
        kills = request.form.get('kills')
        deaths = request.form.get('deaths')

        # Validate kills and deaths
        try:
            kills = int(kills)
            deaths = int(deaths)
            if 0 <= kills <= 50 and 0 <= deaths <= 50:
                description = f"{tournament} {stage} {match_type} {match_name}"
                database.add_match(description, map_name, player, kills, deaths)
                flash('Match added successfully!')
            else:
                flash('Invalid kills or deaths. Must be between 0 and 50.')
        except ValueError:
            flash('Kills and deaths must be valid integers.')

        # Render update.html with form values persisted
        scores = database.get_scores()
        max_count = max([info['count'] for info in scores.values()]) if scores else 1
        return render_template('update.html',
                               scores=scores,
                               max_count=max_count,
                               tournament=tournament or '',
                               stage=stage or '',
                               match_type=match_type or '',
                               match_name=match_name or '',
                               map_name=map_name or '',
                               player=player or '',
                               kills=kills or '',
                               deaths=deaths or '')

    # GET: Check if authenticated or validate password
    if 'authenticated' not in session:
        password = request.args.get('password')
        if not password or not bcrypt.checkpw(password.encode('utf-8'), PASSWORD_HASH):
            flash('Please provide a valid password to access the update page.')
            return redirect(url_for('index'))
        session['authenticated'] = True  # Mark session as authenticated

    # Render update page
    scores = database.get_scores()
    max_count = max([info['count'] for info in scores.values()]) if scores else 1
    return render_template('update.html',
                           scores=scores,
                           max_count=max_count,
                           tournament='',
                           stage='',
                           match_type='',
                           match_name='',
                           map_name='',
                           player='',
                           kills='',
                           deaths='')
if __name__ == '__main__':
    app.run(debug=True)
