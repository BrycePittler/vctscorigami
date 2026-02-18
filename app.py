from flask import Flask, render_template, request, redirect, url_for, flash, session
import database
import bcrypt
import os

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(24))

PASSWORD_HASH = b"$2b$12$UxfOKV7MadrhIWPhy1Sozu3r0fhwr8pgshjd9t08XhSEvA791fWZO"

# Initialize database on startup
database.init_db()

# Check if using PostgreSQL
USE_POSTGRES = os.environ.get('DATABASE_URL') is not None


def execute_db(conn, query, params=()):
    """Execute a query with proper cursor handling."""
    if USE_POSTGRES:
        cur = conn.cursor()
        cur.execute(query, params)
        return cur
    else:
        return conn.execute(query, params)


def rank_leaderboard(leaderboard_data, score_key):
    if not leaderboard_data:
        return []
    
    ranked_list = []
    current_rank = 1
    
    sorted_data = sorted(leaderboard_data, key=lambda x: x[score_key], reverse=True)
    
    if sorted_data:
        ranked_list.append({'rank': current_rank, **sorted_data[0]})
        
        for i in range(1, len(sorted_data)):
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
    
    scores = database.get_scores(
        selected_player if selected_player != 'all' else None,
        selected_tournament if selected_tournament != 'all' else None
    )
    
    max_count = max([info['count'] for info in scores.values()]) if scores else 1
    
    conn = database.get_db_connection()
    
    # Get scorigami scores
    cur = execute_db(conn, '''
        SELECT kills, deaths
        FROM matches
        GROUP BY kills, deaths
        HAVING COUNT(*) = 1
    ''')
    overall_scorigamis_raw = cur.fetchall()
    if USE_POSTGRES:
        cur.close()
    
    overall_scorigamis = {(s[0], s[1]) for s in overall_scorigamis_raw}
    
    # Get unique players - PostgreSQL-safe query
    cur = execute_db(conn, '''
        SELECT player FROM matches 
        GROUP BY player 
        ORDER BY LOWER(player)
    ''')
    unique_players = cur.fetchall()
    if USE_POSTGRES:
        cur.close()
    
    unique_tournaments = database.get_unique_tournaments_list()
    
    # Leaderboard queries
    cur = execute_db(conn, '''
        SELECT player, COUNT(DISTINCT kills::text || '-' || deaths::text) as unique_scores
        FROM matches
        GROUP BY player
        ORDER BY unique_scores DESC
    ''') if USE_POSTGRES else execute_db(conn, '''
        SELECT player, COUNT(DISTINCT kills || '-' || deaths) as unique_scores
        FROM matches
        GROUP BY player
        ORDER BY unique_scores DESC
    ''')
    leaderboard_unique_raw = cur.fetchall()
    if USE_POSTGRES:
        cur.close()
    
    cur = execute_db(conn, '''
        SELECT player, COUNT(DISTINCT kills::text || '-' || deaths::text) as exclusive_scores
        FROM matches m1
        WHERE NOT EXISTS (
            SELECT 1 FROM matches m2 
            WHERE m2.kills = m1.kills AND m2.deaths = m1.deaths AND m2.player != m1.player
        )
        GROUP BY player
        ORDER BY exclusive_scores DESC
    ''') if USE_POSTGRES else execute_db(conn, '''
        SELECT player, COUNT(DISTINCT kills || '-' || deaths) as exclusive_scores
        FROM matches m1
        WHERE NOT EXISTS (
            SELECT 1 FROM matches m2 
            WHERE m2.kills = m1.kills AND m2.deaths = m1.deaths AND m2.player != m1.player
        )
        GROUP BY player
        ORDER BY exclusive_scores DESC
    ''')
    leaderboard_exclusive_raw = cur.fetchall()
    if USE_POSTGRES:
        cur.close()
    
    cur = execute_db(conn, '''
        SELECT player, COUNT(*) as total_matches
        FROM matches
        GROUP BY player
        ORDER BY total_matches DESC
    ''')
    leaderboard_maps_played_raw = cur.fetchall()
    if USE_POSTGRES:
        cur.close()
    
    cur = execute_db(conn, '''
        SELECT player, SUM(kills) - SUM(deaths) AS kill_death_difference
        FROM matches
        GROUP BY player
        ORDER BY kill_death_difference DESC
    ''')
    leaderboard_kd_raw = cur.fetchall()
    if USE_POSTGRES:
        cur.close()

    # Get total kills and deaths
    query = 'SELECT SUM(kills) as total_kills, SUM(deaths) as total_deaths FROM matches'
    params = []
    conditions = []
    if selected_player != 'all':
        conditions.append('player = %s' if USE_POSTGRES else 'player = ?')
        params.append(selected_player)
    if selected_tournament != 'all':
        conditions.append('description LIKE %s' if USE_POSTGRES else 'description LIKE ?')
        params.append(f'%{selected_tournament}%')
    if conditions:
        query += ' WHERE ' + ' AND '.join(conditions)
    
    cur = execute_db(conn, query, params) if params else execute_db(conn, query)
    totals = cur.fetchone()
    if USE_POSTGRES:
        cur.close()
    
    total_kills = totals[0] if totals and totals[0] else 0
    total_deaths = totals[1] if totals and totals[1] else 0

    conn.close()
    
    # Convert tuples to dicts for ranking
    leaderboard_unique_raw = [{'player': r[0], 'unique_scores': r[1]} for r in leaderboard_unique_raw]
    leaderboard_exclusive_raw = [{'player': r[0], 'exclusive_scores': r[1]} for r in leaderboard_exclusive_raw]
    leaderboard_maps_played_raw = [{'player': r[0], 'total_matches': r[1]} for r in leaderboard_maps_played_raw]
    leaderboard_kd_raw = [{'player': r[0], 'kill_death_difference': r[1]} for r in leaderboard_kd_raw]
    
    leaderboard_unique = rank_leaderboard(leaderboard_unique_raw, 'unique_scores')
    leaderboard_exclusive = rank_leaderboard(leaderboard_exclusive_raw, 'exclusive_scores')
    leaderboard_maps_played = rank_leaderboard(leaderboard_maps_played_raw, 'total_matches')
    leaderboard_kd = rank_leaderboard(leaderboard_kd_raw, 'kill_death_difference')

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
    if request.method == 'POST':
        if 'authenticated' not in session:
            password = request.form.get('password')
            if not password or not bcrypt.checkpw(password.encode('utf-8'), PASSWORD_HASH):
                flash('Incorrect password. Access denied.')
                return redirect(url_for('index'))
            session['authenticated'] = True

        tournament = request.form.get('tournament')
        stage = request.form.get('stage')
        match_type = request.form.get('match_type')
        match_name = request.form.get('match_name')
        map_name = request.form.get('map')
        player = request.form.get('player')
        kills = request.form.get('kills')
        deaths = request.form.get('deaths')
        match_date = request.form.get('match_date')
        result = request.form.get('result')
        team = request.form.get('team')

        try:
            kills = int(kills)
            deaths = int(deaths)
            if 0 <= kills <= 50 and 0 <= deaths <= 50:
                description = f"{tournament} {stage} {match_type} {match_name}"
                database.add_matches_batch([{
                    'description': description,
                    'map': map_name,
                    'player': player,
                    'kills': kills,
                    'deaths': deaths,
                    'match_date': match_date,
                    'result': result,
                    'team': team
                }])
                flash('Match added successfully!')
            else:
                flash('Invalid kills or deaths. Must be between 0 and 50.')
        except ValueError:
            flash('Kills and deaths must be valid integers.')

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
                               deaths=deaths or '',
                               match_date=match_date or '',
                               result=result or '',
                               team=team or '')

    if 'authenticated' not in session:
        password = request.args.get('password')
        if not password or not bcrypt.checkpw(password.encode('utf-8'), PASSWORD_HASH):
            flash('Please provide a valid password to access the update page.')
            return redirect(url_for('index'))
        session['authenticated'] = True

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
                           deaths='',
                           match_date='',
                           result='',
                           team='')


@app.route('/admin')
def admin():
    stats = database.get_database_stats()
    return render_template('admin.html', stats=stats)


@app.route('/admin/fetch/<int:tournament_id>')
def admin_fetch_tournament(tournament_id):
    from data_fetcher import fetch_tournament_data
    
    try:
        matches = fetch_tournament_data(tournament_id)
        if matches:
            inserted, skipped = database.add_matches_batch(matches)
            flash(f'Fetched {len(matches)} records. Inserted: {inserted}, Skipped: {skipped}')
        else:
            flash(f'No data found for tournament {tournament_id}')
    except Exception as e:
        flash(f'Error: {str(e)}')
    
    return redirect(url_for('admin'))


@app.route('/admin/fetch-all')
def admin_fetch_all():
    from data_fetcher import fetch_all_tier1_data
    
    try:
        matches = fetch_all_tier1_data(delay=0.5)
        if matches:
            inserted, skipped = database.add_matches_batch(matches)
            flash(f'Fetched {len(matches)} records. Inserted: {inserted}, Skipped: {skipped}')
        else:
            flash('No data found')
    except Exception as e:
        flash(f'Error: {str(e)}')
    
    return redirect(url_for('admin'))


if __name__ == '__main__':
    app.run(debug=True)