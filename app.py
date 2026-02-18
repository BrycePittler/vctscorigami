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
    """
    Assigns ranks to a leaderboard list, handling ties correctly.
    Players with the same score get the same rank and same rank color.
    """
    if not leaderboard_data:
        return []
    
    ranked_list = []
    
    # Sort the data in descending order of the score
    sorted_data = sorted(leaderboard_data, key=lambda x: x[score_key], reverse=True)
    
    if sorted_data:
        current_rank = 1
        prev_score = None
        count_at_rank = 0
        
        for i, item in enumerate(sorted_data):
            # Check for a tie with the previous entry
            if prev_score is not None and item[score_key] != prev_score:
                # New score, update rank (skip numbers based on how many had previous score)
                current_rank = i + 1
            
            prev_score = item[score_key]
            
            # Determine rank class for coloring
            if current_rank == 1:
                rank_class = 'gold'
            elif current_rank == 2:
                rank_class = 'silver'
            elif current_rank == 3:
                rank_class = 'bronze'
            else:
                rank_class = ''
            
            ranked_list.append({
                'rank': current_rank, 
                'rank_class': rank_class,
                **item
            })
                
    return ranked_list

@app.route('/')
def index():
    selected_view = request.args.get('view', 'gradient')
    selected_player = request.args.get('player', 'all')
    selected_team1 = request.args.get('team1', 'all')
    selected_team2 = request.args.get('team2', 'all')
    timeline_value = int(request.args.get('timeline', 100))
    
    conn = database.get_db_connection()
    
    # Get date range
    date_range = conn.execute('SELECT MIN(match_date) as min_date, MAX(match_date) as max_date FROM matches').fetchone()
    min_date = date_range['min_date'] or '2023-01-01'
    max_date = date_range['max_date'] or '2026-12-31'
    
    # Calculate cutoff date based on timeline slider
    if timeline_value < 100:
        from datetime import datetime, timedelta
        start = datetime.strptime(min_date, '%Y-%m-%d')
        end = datetime.strptime(max_date, '%Y-%m-%d')
        diff = (end - start).days
        cutoff_days = int(diff * timeline_value / 100)
        cutoff_date = (start + timedelta(days=cutoff_days)).strftime('%Y-%m-%d')
    else:
        cutoff_date = None
    
    # Build filter conditions
    conditions = []
    params = []
    
    if selected_player != 'all':
        conditions.append('player = ?')
        params.append(selected_player)
    
    if selected_team1 != 'all':
        conditions.append('team = ?')
        params.append(selected_team1)
    
    if selected_team2 != 'all':
        conditions.append('description LIKE ?')
        params.append(f'%{selected_team2}%')
    
    if cutoff_date:
        conditions.append('match_date <= ?')
        params.append(cutoff_date)
    
    # Get scores with filters
    where_clause = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
    
    query = f'''
        SELECT kills, deaths, player, map, team, result, match_date, description
        FROM matches
        {where_clause}
    '''
    rows = conn.execute(query, params).fetchall()
    
    # Group by kills, deaths
    scores = {}
    for row in rows:
        key = (row['kills'], row['deaths'])
        if key not in scores:
            scores[key] = {'count': 0, 'matches': [], 'wins': 0, 'total_with_result': 0}
        
        scores[key]['count'] += 1
        
        # Track wins for win percentage
        if row['result']:
            scores[key]['total_with_result'] += 1
            if row['result'] == 'Win':
                scores[key]['wins'] += 1
        
        formatted = f"{row['player']} on {row['map']}"
        if row['result']:
            formatted += f" | {row['result']}"
        if row['match_date']:
            formatted += f" | {row['match_date']}"
        if row['team']:
            formatted += f"\nTeam: {row['team']}"
        if row['description']:
            formatted += f"\n{row['description']}"
        
        if formatted not in scores[key]['matches']:
            scores[key]['matches'].append(formatted)
    
    # Calculate win percentages and finalize
    for key in scores:
        scores[key]['details'] = '\n\n'.join(scores[key]['matches'])
        if scores[key]['total_with_result'] > 0:
            scores[key]['win_pct'] = (scores[key]['wins'] / scores[key]['total_with_result']) * 100
        else:
            scores[key]['win_pct'] = None
    
    max_count = max([info['count'] for info in scores.values()]) if scores else 1
    
    # Get scorigamis (filtered)
    scorigami_query = f'''
        SELECT kills, deaths
        FROM matches
        {where_clause}
        GROUP BY kills, deaths
        HAVING COUNT(*) = 1
    '''
    overall_scorigamis_raw = conn.execute(scorigami_query, params).fetchall()
    overall_scorigamis = {(s['kills'], s['deaths']) for s in overall_scorigamis_raw}
    
    # Get unique players and teams
    unique_players = conn.execute('SELECT DISTINCT player FROM matches ORDER BY LOWER(player)').fetchall()
    unique_teams = conn.execute('SELECT DISTINCT team FROM matches WHERE team IS NOT NULL AND team != "" ORDER BY team').fetchall()
    unique_teams = [row['team'] for row in unique_teams]
    
    # Get totals
    total_query = 'SELECT SUM(kills) as total_kills, SUM(deaths) as total_deaths FROM matches' + where_clause
    totals = conn.execute(total_query, params).fetchone()
    total_kills = totals['total_kills'] if totals and totals['total_kills'] else 0
    total_deaths = totals['total_deaths'] if totals and totals['total_deaths'] else 0
    
    # Leaderboards (filtered)
    leaderboard_unique = rank_leaderboard(conn.execute(f'''
        SELECT player, COUNT(DISTINCT kills || '-' || deaths) as unique_scores
        FROM matches {where_clause} GROUP BY player ORDER BY unique_scores DESC
    ''', params).fetchall(), 'unique_scores')
    
    leaderboard_exclusive = rank_leaderboard(conn.execute(f'''
        SELECT player, COUNT(DISTINCT kills || '-' || deaths) as exclusive_scores
        FROM matches m1
        {where_clause.replace('WHERE', 'WHERE') if conditions else ''}
        {' AND ' if conditions else 'WHERE NOT '} EXISTS (
            SELECT 1 FROM matches m2 
            WHERE m2.kills = m1.kills AND m2.deaths = m1.deaths AND m2.player != m1.player
        )
        GROUP BY player ORDER BY exclusive_scores DESC
    ''', params).fetchall() if conditions else conn.execute('''
        SELECT player, COUNT(DISTINCT kills || '-' || deaths) as exclusive_scores
        FROM matches m1
        WHERE NOT EXISTS (
            SELECT 1 FROM matches m2 
            WHERE m2.kills = m1.kills AND m2.deaths = m1.deaths AND m2.player != m1.player
        )
        GROUP BY player ORDER BY exclusive_scores DESC
    ''').fetchall(), 'exclusive_scores')
    
    leaderboard_maps_played = rank_leaderboard(conn.execute(f'''
        SELECT player, COUNT(*) as total_matches
        FROM matches {where_clause} GROUP BY player ORDER BY total_matches DESC
    ''', params).fetchall(), 'total_matches')
    
    leaderboard_kd = rank_leaderboard(conn.execute(f'''
        SELECT player, SUM(kills) - SUM(deaths) AS kill_death_difference
        FROM matches {where_clause} GROUP BY player ORDER BY kill_death_difference DESC
    ''', params).fetchall(), 'kill_death_difference')
    
    conn.close()
    
    return render_template('index.html', 
        scores=scores, 
        max_count=max_count,
        leaderboard_unique=leaderboard_unique, 
        leaderboard_exclusive=leaderboard_exclusive,
        leaderboard_maps_played=leaderboard_maps_played,
        leaderboard_kd=leaderboard_kd,
        unique_players=unique_players, 
        unique_teams=unique_teams,
        selected_view=selected_view,
        selected_player=selected_player, 
        selected_team1=selected_team1,
        selected_team2=selected_team2,
        timeline_value=timeline_value,
        min_date=min_date,
        max_date=max_date,
        total_kills=total_kills,
        total_deaths=total_deaths,
        overall_scorigamis=overall_scorigamis
    )

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