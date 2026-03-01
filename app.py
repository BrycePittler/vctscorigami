from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
import database
import bcrypt
import os

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(24))

PASSWORD_HASH = b"$2b$12$UxfOKV7MadrhIWPhy1Sozu3r0fhwr8pgshjd9t08XhSEvA791fWZO"

# Initialize database on startup
database.init_db()


def rank_leaderboard(leaderboard_data, score_key):
    if not leaderboard_data:
        return []
    ranked_list = []
    sorted_data = sorted(leaderboard_data, key=lambda x: x[score_key], reverse=True)
    if sorted_data:
        current_rank = 1
        prev_score = None
        for i, item in enumerate(sorted_data):
            if prev_score is not None and item[score_key] != prev_score:
                current_rank = i + 1
            prev_score = item[score_key]
            rank_class = 'gold' if current_rank == 1 else 'silver' if current_rank == 2 else 'bronze' if current_rank == 3 else ''
            ranked_list.append({'rank': current_rank, 'rank_class': rank_class, **item})
    return ranked_list

@app.route('/')
def index():
    selected_view = request.args.get('view', 'gradient')
    selected_player = request.args.get('player', 'all')
    selected_team1 = request.args.get('team1', 'all')
    selected_team2 = request.args.get('team2', 'all')
    timeline_value = int(request.args.get('timeline', 100))
    
    conn = database.get_db_connection()
    
    date_range = database.fetchone(conn, 'SELECT MIN(match_date) as min_date, MAX(match_date) as max_date FROM matches')
    min_date = date_range['min_date'] or '2023-01-01'
    max_date = date_range['max_date'] or '2026-12-31'
    
    if timeline_value < 100:
        from datetime import datetime, timedelta
        start = datetime.strptime(min_date, '%Y-%m-%d')
        end = datetime.strptime(max_date, '%Y-%m-%d')
        diff = (end - start).days
        cutoff_days = int(diff * timeline_value / 100)
        cutoff_date = (start + timedelta(days=cutoff_days)).strftime('%Y-%m-%d')
    else:
        cutoff_date = None
    
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
    
    where_clause = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
    
    rows = database.fetchall(conn, f'SELECT kills, deaths, player, map, team, result, match_date, description FROM matches {where_clause}', params)
    
    scores = {}
    for row in rows:
        key = (row['kills'], row['deaths'])
        if key not in scores:
            scores[key] = {'count': 0, 'matches': [], 'wins': 0, 'total_with_result': 0}
        scores[key]['count'] += 1
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
    
    for key in scores:
        scores[key]['details'] = '\n\n'.join(scores[key]['matches'])
        scores[key]['win_pct'] = (scores[key]['wins'] / scores[key]['total_with_result'] * 100) if scores[key]['total_with_result'] > 0 else None
    
    max_count = max([info['count'] for info in scores.values()]) if scores else 1
    
    overall_scorigamis_raw = database.fetchall(conn, f'SELECT kills, deaths FROM matches {where_clause} GROUP BY kills, deaths HAVING COUNT(*) = 1', params)
    overall_scorigamis = {(s['kills'], s['deaths']) for s in overall_scorigamis_raw}
    
    # PostgreSQL: Use GROUP BY instead of SELECT DISTINCT, use '' instead of ""
    unique_players = database.fetchall(conn, "SELECT player FROM matches GROUP BY player ORDER BY MIN(LOWER(player))")
    unique_teams_raw = database.fetchall(conn, "SELECT team FROM matches WHERE team IS NOT NULL AND team != '' GROUP BY team ORDER BY team")
    unique_teams = [row['team'] for row in unique_teams_raw]
    
    recent_scorigamis_raw = database.fetchall(conn, '''
        SELECT m.kills, m.deaths, m.player, m.map, m.team, m.result, m.match_date, m.description
        FROM matches m
        INNER JOIN (SELECT kills, deaths FROM matches GROUP BY kills, deaths HAVING COUNT(*) = 1) unique_scores 
        ON m.kills = unique_scores.kills AND m.deaths = unique_scores.deaths
        ORDER BY m.match_date DESC
    ''')
    
    recent_scorigamis = []
    for row in recent_scorigamis_raw:
        opponent = None
        if row['description'] and row['team'] and ' vs ' in row['description']:
            parts = row['description'].split(' vs ')
            if len(parts) > 1:
                team2 = parts[1].strip()
                before_vs = parts[0].strip()
                if row['team'] == team2:
                    words = before_vs.split()
                    for num_words in range(min(4, len(words)), 0, -1):
                        potential_team = ' '.join(words[-num_words:])
                        if potential_team in unique_teams and potential_team != row['team']:
                            opponent = potential_team
                            break
                else:
                    opponent = team2
        recent_scorigamis.append({k: row[k] for k in row} | {'opponent': opponent})
    
    totals = database.fetchone(conn, f'SELECT SUM(kills) as total_kills, SUM(deaths) as total_deaths FROM matches {where_clause}', params)
    total_kills = totals['total_kills'] or 0
    total_deaths = totals['total_deaths'] or 0
    
    leaderboard_total_kills = rank_leaderboard(database.fetchall(conn, f'SELECT player, SUM(kills) as total_kills FROM matches {where_clause} GROUP BY player ORDER BY total_kills DESC', params), 'total_kills')
    
    if conditions:
        leaderboard_exclusive = rank_leaderboard(database.fetchall(conn, f'''
            SELECT player, COUNT(DISTINCT kills || '-' || deaths) as exclusive_scores FROM matches m1
            {where_clause} AND NOT EXISTS (SELECT 1 FROM matches m2 WHERE m2.kills = m1.kills AND m2.deaths = m1.deaths AND m2.player != m1.player)
            GROUP BY player ORDER BY exclusive_scores DESC
        ''', params), 'exclusive_scores')
    else:
        leaderboard_exclusive = rank_leaderboard(database.fetchall(conn, '''
            SELECT player, COUNT(DISTINCT kills || '-' || deaths) as exclusive_scores FROM matches m1
            WHERE NOT EXISTS (SELECT 1 FROM matches m2 WHERE m2.kills = m1.kills AND m2.deaths = m1.deaths AND m2.player != m1.player)
            GROUP BY player ORDER BY exclusive_scores DESC
        '''), 'exclusive_scores')
    
    leaderboard_maps_played = rank_leaderboard(database.fetchall(conn, f'SELECT player, COUNT(*) as total_matches FROM matches {where_clause} GROUP BY player ORDER BY total_matches DESC', params), 'total_matches')
    leaderboard_kd = rank_leaderboard(database.fetchall(conn, f'SELECT player, SUM(kills) - SUM(deaths) AS kill_death_difference FROM matches {where_clause} GROUP BY player ORDER BY kill_death_difference DESC', params), 'kill_death_difference')
    
    conn.close()
    
    return render_template('index.html', scores=scores, max_count=max_count, leaderboard_total_kills=leaderboard_total_kills,
        leaderboard_exclusive=leaderboard_exclusive, leaderboard_maps_played=leaderboard_maps_played, leaderboard_kd=leaderboard_kd,
        unique_players=unique_players, unique_teams=unique_teams, selected_view=selected_view, selected_player=selected_player,
        selected_team1=selected_team1, selected_team2=selected_team2, timeline_value=timeline_value, min_date=min_date, max_date=max_date,
        total_kills=total_kills, total_deaths=total_deaths, overall_scorigamis=overall_scorigamis, recent_scorigamis=recent_scorigamis)

@app.route('/api/data')
def api_data():
    selected_player = request.args.get('player', 'all')
    selected_team1 = request.args.get('team1', 'all')
    selected_team2 = request.args.get('team2', 'all')
    timeline_value = int(request.args.get('timeline', 100))
    
    conn = database.get_db_connection()
    
    date_range = database.fetchone(conn, 'SELECT MIN(match_date) as min_date, MAX(match_date) as max_date FROM matches')
    min_date = date_range['min_date'] or '2023-01-01'
    max_date = date_range['max_date'] or '2026-12-31'
    
    if timeline_value < 100:
        from datetime import datetime, timedelta
        start = datetime.strptime(min_date, '%Y-%m-%d')
        end = datetime.strptime(max_date, '%Y-%m-%d')
        cutoff_date = (start + timedelta(days=int((end - start).days * timeline_value / 100))).strftime('%Y-%m-%d')
    else:
        cutoff_date = None
    
    conditions, params = [], []
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
    
    where_clause = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
    
    rows = database.fetchall(conn, f'SELECT kills, deaths, player, map, team, result, match_date, description FROM matches {where_clause}', params)
    
    scores = {}
    for row in rows:
        key = (row['kills'], row['deaths'])
        if key not in scores:
            scores[key] = {'count': 0, 'matches': [], 'wins': 0, 'total_with_result': 0}
        scores[key]['count'] += 1
        if row['result']:
            scores[key]['total_with_result'] += 1
            if row['result'] == 'Win':
                scores[key]['wins'] += 1
        formatted = f"{row['player']} on {row['map']}"
        if row['result']: formatted += f" | {row['result']}"
        if row['match_date']: formatted += f" | {row['match_date']}"
        if row['team']: formatted += f"\nTeam: {row['team']}"
        if row['description']: formatted += f"\n{row['description']}"
        if formatted not in scores[key]['matches']:
            scores[key]['matches'].append(formatted)
    
    for key in scores:
        scores[key]['details'] = '\n\n'.join(scores[key]['matches'])
        scores[key]['win_pct'] = (scores[key]['wins'] / scores[key]['total_with_result'] * 100) if scores[key]['total_with_result'] > 0 else None
    
    max_count = max([info['count'] for info in scores.values()]) if scores else 1
    
    overall_scorigamis_raw = database.fetchall(conn, f'SELECT kills, deaths FROM matches {where_clause} GROUP BY kills, deaths HAVING COUNT(*) = 1', params)
    overall_scorigamis = [[s['kills'], s['deaths']] for s in overall_scorigamis_raw]
    
    unique_players = database.fetchall(conn, "SELECT player FROM matches GROUP BY player ORDER BY MIN(LOWER(player))")
    unique_teams_raw = database.fetchall(conn, "SELECT team FROM matches WHERE team IS NOT NULL AND team != '' GROUP BY team ORDER BY team")
    unique_teams = [row['team'] for row in unique_teams_raw]
    
    recent_scorigamis_raw = database.fetchall(conn, '''
        SELECT m.kills, m.deaths, m.player, m.map, m.team, m.result, m.match_date, m.description
        FROM matches m INNER JOIN (SELECT kills, deaths FROM matches GROUP BY kills, deaths HAVING COUNT(*) = 1) unique_scores 
        ON m.kills = unique_scores.kills AND m.deaths = unique_scores.deaths ORDER BY m.match_date DESC
    ''')
    
    recent_scorigamis = []
    for row in recent_scorigamis_raw:
        opponent = None
        if row['description'] and row['team'] and ' vs ' in row['description']:
            parts = row['description'].split(' vs ')
            if len(parts) > 1:
                team2 = parts[1].strip()
                before_vs = parts[0].strip()
                if row['team'] == team2:
                    words = before_vs.split()
                    for num_words in range(min(4, len(words)), 0, -1):
                        potential_team = ' '.join(words[-num_words:])
                        if potential_team in unique_teams and potential_team != row['team']:
                            opponent = potential_team
                            break
                else:
                    opponent = team2
        recent_scorigamis.append({k: row[k] for k in row} | {'opponent': opponent})
    
    totals = database.fetchone(conn, f'SELECT SUM(kills) as total_kills, SUM(deaths) as total_deaths FROM matches {where_clause}', params)
    total_kills = totals['total_kills'] or 0
    total_deaths = totals['total_deaths'] or 0
    
    leaderboard_total_kills = rank_leaderboard(database.fetchall(conn, f'SELECT player, SUM(kills) as total_kills FROM matches {where_clause} GROUP BY player ORDER BY total_kills DESC', params), 'total_kills')
    
    if conditions:
        leaderboard_exclusive = rank_leaderboard(database.fetchall(conn, f'''
            SELECT player, COUNT(DISTINCT kills || '-' || deaths) as exclusive_scores FROM matches m1
            {where_clause} AND NOT EXISTS (SELECT 1 FROM matches m2 WHERE m2.kills = m1.kills AND m2.deaths = m1.deaths AND m2.player != m1.player)
            GROUP BY player ORDER BY exclusive_scores DESC
        ''', params), 'exclusive_scores')
    else:
        leaderboard_exclusive = rank_leaderboard(database.fetchall(conn, '''
            SELECT player, COUNT(DISTINCT kills || '-' || deaths) as exclusive_scores FROM matches m1
            WHERE NOT EXISTS (SELECT 1 FROM matches m2 WHERE m2.kills = m1.kills AND m2.deaths = m1.deaths AND m2.player != m1.player)
            GROUP BY player ORDER BY exclusive_scores DESC
        '''), 'exclusive_scores')
    
    leaderboard_maps_played = rank_leaderboard(database.fetchall(conn, f'SELECT player, COUNT(*) as total_matches FROM matches {where_clause} GROUP BY player ORDER BY total_matches DESC', params), 'total_matches')
    leaderboard_kd = rank_leaderboard(database.fetchall(conn, f'SELECT player, SUM(kills) - SUM(deaths) AS kill_death_difference FROM matches {where_clause} GROUP BY player ORDER BY kill_death_difference DESC', params), 'kill_death_difference')
    
    conn.close()
    
    scores_json = {f"{k},{d}": {'count': info['count'], 'details': info['details'], 'win_pct': info['win_pct']} for (k, d), info in scores.items()}
    
    return jsonify({
        'scores': scores_json, 'max_count': max_count, 'overall_scorigamis': overall_scorigamis,
        'leaderboard_total_kills': leaderboard_total_kills, 'leaderboard_exclusive': leaderboard_exclusive,
        'leaderboard_maps_played': leaderboard_maps_played, 'leaderboard_kd': leaderboard_kd,
        'total_kills': total_kills, 'total_deaths': total_deaths, 'recent_scorigamis': recent_scorigamis,
        'unique_players': [p['player'] for p in unique_players], 'unique_teams': unique_teams
    })

@app.route('/update', methods=['GET', 'POST'])
def update():
    if request.method == 'POST':
        if 'authenticated' not in session:
            password = request.form.get('password')
            if not password or not bcrypt.checkpw(password.encode('utf-8'), PASSWORD_HASH):
                flash('Incorrect password. Access denied.')
                return redirect(url_for('index'))
            session['authenticated'] = True
        try:
            kills = int(request.form.get('kills'))
            deaths = int(request.form.get('deaths'))
            if 0 <= kills <= 50 and 0 <= deaths <= 50:
                database.add_matches_batch([{
                    'description': f"{request.form.get('tournament')} {request.form.get('stage')} {request.form.get('match_type')} {request.form.get('match_name')}",
                    'map': request.form.get('map'), 'player': request.form.get('player'), 'kills': kills, 'deaths': deaths,
                    'match_date': request.form.get('match_date'), 'result': request.form.get('result'), 'team': request.form.get('team')
                }])
                flash('Match added successfully!')
            else:
                flash('Invalid kills or deaths. Must be between 0 and 50.')
        except ValueError:
            flash('Kills and deaths must be valid integers.')
        scores = database.get_scores()
        return render_template('update.html', scores=scores, max_count=max([info['count'] for info in scores.values()]) if scores else 1)
    
    if 'authenticated' not in session:
        password = request.args.get('password')
        if not password or not bcrypt.checkpw(password.encode('utf-8'), PASSWORD_HASH):
            flash('Please provide a valid password to access the update page.')
            return redirect(url_for('index'))
        session['authenticated'] = True
    scores = database.get_scores()
    return render_template('update.html', scores=scores, max_count=max([info['count'] for info in scores.values()]) if scores else 1)

@app.route('/admin')
def admin():
    return render_template('admin.html', stats=database.get_database_stats())

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
