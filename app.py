from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
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
    unique_teams_raw = conn.execute('SELECT DISTINCT team FROM matches WHERE team IS NOT NULL AND team != "" ORDER BY team').fetchall()
    unique_teams = [row['team'] for row in unique_teams_raw]
    
    # Get ALL recent scorigamis ordered by match date (most recent unique kill/death combinations)
    # These are matches where the kills/deaths combo has only occurred once globally
    recent_scorigamis_query = '''
        SELECT m.kills, m.deaths, m.player, m.map, m.team, m.result, m.match_date, m.description
        FROM matches m
        INNER JOIN (
            SELECT kills, deaths
            FROM matches
            GROUP BY kills, deaths
            HAVING COUNT(*) = 1
        ) unique_scores ON m.kills = unique_scores.kills AND m.deaths = unique_scores.deaths
        ORDER BY m.match_date DESC
    '''
    recent_scorigamis_raw = conn.execute(recent_scorigamis_query).fetchall()
    
    recent_scorigamis = []
    for row in recent_scorigamis_raw:
        # Extract opponent team from description
        # Description format: "Tournament Stage Type Team1 vs Team2"
        opponent = None
        if row['description'] and row['team']:
            desc = row['description']
            if ' vs ' in desc:
                parts = desc.split(' vs ')
                if len(parts) > 1:
                    team2 = parts[1].strip()
                    before_vs = parts[0].strip()
                    
                    # If player's team matches team2, find opponent in before_vs
                    if row['team'] == team2:
                        # Try to match known team names at the end of before_vs
                        words = before_vs.split()
                        found_opponent = None
                        
                        # Try to find a known team name (check longest matches first)
                        for num_words in range(min(4, len(words)), 0, -1):
                            potential_team = ' '.join(words[-num_words:])
                            if potential_team in unique_teams and potential_team != row['team']:
                                found_opponent = potential_team
                                break
                        
                        opponent = found_opponent
                    else:
                        # Player is on team1, opponent is team2
                        opponent = team2
        
        recent_scorigamis.append({
            'kills': row['kills'],
            'deaths': row['deaths'],
            'player': row['player'],
            'map': row['map'],
            'team': row['team'],
            'opponent': opponent,
            'result': row['result'],
            'match_date': row['match_date'],
            'description': row['description']
        })
    
    # Get totals
    total_query = 'SELECT SUM(kills) as total_kills, SUM(deaths) as total_deaths FROM matches' + where_clause
    totals = conn.execute(total_query, params).fetchone()
    total_kills = totals['total_kills'] if totals and totals['total_kills'] else 0
    total_deaths = totals['total_deaths'] if totals and totals['total_deaths'] else 0
    
    # Leaderboards (filtered)
    leaderboard_total_kills = rank_leaderboard(conn.execute(f'''
        SELECT player, SUM(kills) as total_kills
        FROM matches {where_clause} GROUP BY player ORDER BY total_kills DESC
    ''', params).fetchall(), 'total_kills')
    
    # Scorigami Leaders: players with unique K/D combos that no other player has (within filtered data)
    # First find all K/D combos that only one player has achieved
    if conditions:
        leaderboard_exclusive = rank_leaderboard(conn.execute(f'''
            SELECT player, COUNT(DISTINCT kills || '-' || deaths) as exclusive_scores
            FROM matches m1
            {where_clause}
            AND NOT EXISTS (
                SELECT 1 FROM matches m2 
                WHERE m2.kills = m1.kills AND m2.deaths = m1.deaths AND m2.player != m1.player
            )
            GROUP BY player ORDER BY exclusive_scores DESC
        ''', params).fetchall(), 'exclusive_scores')
    else:
        leaderboard_exclusive = rank_leaderboard(conn.execute('''
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
        leaderboard_total_kills=leaderboard_total_kills, 
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
        overall_scorigamis=overall_scorigamis,
        recent_scorigamis=recent_scorigamis
    )

@app.route('/api/data')
def api_data():
    """JSON API endpoint for filtered data - enables AJAX updates without page reload."""
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
    overall_scorigamis = [[s['kills'], s['deaths']] for s in overall_scorigamis_raw]
    
    # Get unique players and teams
    unique_players = conn.execute('SELECT DISTINCT player FROM matches ORDER BY LOWER(player)').fetchall()
    unique_teams_raw = conn.execute('SELECT DISTINCT team FROM matches WHERE team IS NOT NULL AND team != "" ORDER BY team').fetchall()
    unique_teams = [row['team'] for row in unique_teams_raw]
    
    # Get ALL recent scorigamis ordered by match date (most recent unique kill/death combinations)
    recent_scorigamis_query = '''
        SELECT m.kills, m.deaths, m.player, m.map, m.team, m.result, m.match_date, m.description
        FROM matches m
        INNER JOIN (
            SELECT kills, deaths
            FROM matches
            GROUP BY kills, deaths
            HAVING COUNT(*) = 1
        ) unique_scores ON m.kills = unique_scores.kills AND m.deaths = unique_scores.deaths
        ORDER BY m.match_date DESC
    '''
    recent_scorigamis_raw = conn.execute(recent_scorigamis_query).fetchall()
    
    recent_scorigamis = []
    for row in recent_scorigamis_raw:
        # Extract opponent team from description
        opponent = None
        if row['description'] and row['team']:
            desc = row['description']
            if ' vs ' in desc:
                parts = desc.split(' vs ')
                if len(parts) > 1:
                    team2 = parts[1].strip()
                    before_vs = parts[0].strip()
                    
                    if row['team'] == team2:
                        words = before_vs.split()
                        found_opponent = None
                        
                        for num_words in range(min(4, len(words)), 0, -1):
                            potential_team = ' '.join(words[-num_words:])
                            if potential_team in unique_teams and potential_team != row['team']:
                                found_opponent = potential_team
                                break
                        
                        opponent = found_opponent
                    else:
                        opponent = team2
        
        recent_scorigamis.append({
            'kills': row['kills'],
            'deaths': row['deaths'],
            'player': row['player'],
            'map': row['map'],
            'team': row['team'],
            'opponent': opponent,
            'result': row['result'],
            'match_date': row['match_date'],
            'description': row['description']
        })
    
    # Get totals
    total_query = 'SELECT SUM(kills) as total_kills, SUM(deaths) as total_deaths FROM matches' + where_clause
    totals = conn.execute(total_query, params).fetchone()
    total_kills = totals['total_kills'] if totals and totals['total_kills'] else 0
    total_deaths = totals['total_deaths'] if totals and totals['total_deaths'] else 0
    
    # Leaderboards (filtered)
    leaderboard_total_kills = rank_leaderboard(conn.execute(f'''
        SELECT player, SUM(kills) as total_kills
        FROM matches {where_clause} GROUP BY player ORDER BY total_kills DESC
    ''', params).fetchall(), 'total_kills')
    
    # Scorigami Leaders
    if conditions:
        leaderboard_exclusive = rank_leaderboard(conn.execute(f'''
            SELECT player, COUNT(DISTINCT kills || '-' || deaths) as exclusive_scores
            FROM matches m1
            {where_clause}
            AND NOT EXISTS (
                SELECT 1 FROM matches m2 
                WHERE m2.kills = m1.kills AND m2.deaths = m1.deaths AND m2.player != m1.player
            )
            GROUP BY player ORDER BY exclusive_scores DESC
        ''', params).fetchall(), 'exclusive_scores')
    else:
        leaderboard_exclusive = rank_leaderboard(conn.execute('''
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
    
    # Convert scores dict keys to strings for JSON
    scores_json = {}
    for (k, d), info in scores.items():
        scores_json[f"{k},{d}"] = {
            'count': info['count'],
            'details': info['details'],
            'win_pct': info['win_pct']
        }
    
    return jsonify({
        'scores': scores_json,
        'max_count': max_count,
        'overall_scorigamis': overall_scorigamis,
        'leaderboard_total_kills': leaderboard_total_kills,
        'leaderboard_exclusive': leaderboard_exclusive,
        'leaderboard_maps_played': leaderboard_maps_played,
        'leaderboard_kd': leaderboard_kd,
        'total_kills': total_kills,
        'total_deaths': total_deaths,
        'recent_scorigamis': recent_scorigamis,
        'unique_players': [p['player'] for p in unique_players],
        'unique_teams': unique_teams
    })


@app.route('/api/kd-race')
def api_kd_race():
    """API endpoint for K-D race chart data - returns cumulative K-D over time for top players."""
    from collections import defaultdict
    import re
    
    # Configuration
    track_n_players = 250  # Track top 250 players by maps played
    
    # Team name mapping - maps alternate names to canonical name (same as team-race)
    team_name_mapping = {
        'VISA KRÜ': 'KRÜ Esports',
        'VISA KRÜ(KRÜ Esports)': 'KRÜ Esports',
        'KRÜ Esports': 'KRÜ Esports',
        'LEVIATÁN': 'Leviatán',
        'LOUD': 'LOUD',
        'FURIA': 'FURIA',
        'FURIA Esports': 'FURIA',
        'MIBR': 'MIBR',
        'MIBR Esports': 'MIBR',
        '100 Thieves': '100 Thieves',
        '100T': '100 Thieves',
        'Cloud9': 'Cloud9',
        'C9': 'Cloud9',
        'Evil Geniuses': 'Evil Geniuses',
        'EG': 'Evil Geniuses',
        'G2 Esports': 'G2 Esports',
        'G2': 'G2 Esports',
        'NRG Esports': 'NRG',
        'NRG': 'NRG',
        'Sentinels': 'Sentinels',
        'SEN': 'Sentinels',
        'FNATIC': 'FNATIC',
        'FNC': 'FNATIC',
        'FUT Esports': 'FUT Esports',
        'FUT': 'FUT Esports',
        'Guild Esports': 'Guild Esports',
        'GUILD': 'Guild Esports',
        'Team Liquid': 'Team Liquid',
        'TL': 'Team Liquid',
        'Team Vitality': 'Team Vitality',
        'Vitality': 'Team Vitality',
        'NaVi': 'Natus Vincere',
        'Natus Vincere': 'Natus Vincere',
        'NAVI': 'Natus Vincere',
        'Fnatic': 'FNATIC',
        'Paper Rex': 'Paper Rex',
        'PRX': 'Paper Rex',
        'DRX': 'DRX',
        'DRX (V/S Gaming)': 'DRX',
        'Dragon Ranger Gaming': 'Dragon Ranger Gaming',
        'DRG': 'Dragon Ranger Gaming',
        'ZETA DIVISION': 'ZETA DIVISION',
        'ZETA': 'ZETA DIVISION',
        'DetonatioN FocusMe': 'DetonatioN FocusMe',
        'DFM': 'DetonatioN FocusMe',
        'T1': 'T1',
        'Gen.G': 'Gen.G',
        'GenG': 'Gen.G',
        'RRQ': 'Rex Regum Qeon',
        'Rex Regum Qeon': 'Rex Regum Qeon',
        'Talon Esports': 'TALON',
        'TALON': 'TALON',
        'TLN': 'TALON',
        'Team Secret': 'Team Secret',
        'TS': 'Team Secret',
        'Global Esports': 'Global Esports',
        'GE': 'Global Esports',
        'BLEED': 'BLEED',
        'Team BLEED': 'BLEED',
        'Edward Gaming': 'Edward Gaming',
        'EDG': 'Edward Gaming',
        'FunPlus Phoenix': 'FunPlus Phoenix',
        'FPX': 'FunPlus Phoenix',
        'Bilibili Gaming': 'Bilibili Gaming',
        'BG': 'Bilibili Gaming',
        'Trace Esports': 'Trace Esports',
        'TE': 'Trace Esports',
        'JDG Gaming': 'JDG Esports',
        'JDG Esports': 'JDG Esports',
        'JDG': 'JDG Esports',
        'Titan FC': 'Titan Esports Club',
        'Titan Esports Club': 'Titan Esports Club',
        'XLG': 'Xi Lai Gaming',
        'Xi Lai Gaming': 'Xi Lai Gaming',
        'Wolves Esports': 'Wolves Esports',
        'WO': 'Wolves Esports',
        'Nova Esports': 'Nova Esports',
        'NOVA': 'Nova Esports',
        'Attack All Around': 'Attack All Around',
        'AAA': 'Attack All Around',
        'ONIC G': 'ONIC G',
        'ONIC': 'ONIC G',
        'Alter Ego': 'Alter Ego',
        'AE': 'Alter Ego',
        'BOOM Esports': 'BOOM Esports',
        'BOOM': 'BOOM Esports',
        'Rise': 'Rise',
        'XSET': 'XSET',
        'The Guard': 'The Guard',
        'Guard': 'The Guard',
        'OpTic Gaming': 'OpTic Gaming',
        'OpTic': 'OpTic Gaming',
        'Optic': 'OpTic Gaming',
        'Version1': 'Version1',
        'V1': 'Version1',
        'Gambit Esports': 'Gambit Esports',
        'Gambit': 'Gambit Esports',
        'Masters Seoul': 'Masters Seoul',
        'Acend': 'Acend',
        'ACE': 'Acend',
        'SuperMassive Blaze': 'SuperMassive Blaze',
        'SMB': 'SuperMassive Blaze',
        'Oxygen Esports': 'Oxygen Esports',
        'OXG': 'Oxygen Esports',
        'G store in': 'G store in',
        'G2 Gozen': 'G2 Gozen',
        'GUILD EK': 'GUILD EK',
        'FOKUS': 'FOKUS',
        'FOKUS ME': 'FOKUS',
        'Heretics': 'Team Heretics',
        'Team Heretics': 'Team Heretics',
        'TH': 'Team Heretics',
        'KOI': 'KOI',
        'Giants': 'Giants',
        'GIANTX': 'Giants',
        'GiantX': 'Giants',
        'Giants Gaming': 'Giants',
        'Vodafone Giants': 'Giants',
        'BBL Esports': 'BBL Esports',
        'BBL': 'BBL Esports',
        'Fire Flux Esports': 'Fire Flux Esports',
        'Fire Flux': 'Fire Flux Esports',
        'S2V Esports': 'S2V Esports',
        'S2V': 'S2V Esports',
        'Case Esports': 'Case Esports',
        'CASE': 'Case Esports',
        'M3 Champions': 'M3 Champions',
        'M3C': 'M3 Champions',
        'LDLC OL': 'LDLC OL',
        'LDLC': 'LDLC OL',
        'Funplus Phoenix': 'FunPlus Phoenix',
        'Liquid': 'Team Liquid',
        'Navi': 'Natus Vincere',
        'Secret': 'Team Secret',
        'Talon': 'TALON',
        'Rex Regum': 'Rex Regum Qeon',
        'Paper': 'Paper Rex',
        'DetonatioN': 'DetonatioN FocusMe',
        'Edward': 'Edward Gaming',
        'Bilibili': 'Bilibili Gaming',
        'Wolves': 'Wolves Esports',
        'TYLOO': 'TYLOO',
        'TyLoo': 'TYLOO',
        'Karmine Corp': 'Karmine Corp',
        'KC': 'Karmine Corp',
        'Gentle Mates': 'Gentle Mates',
        'All Gamers': 'All Gamers',
        'AG': 'All Gamers',
        'Nongshim RedForce': 'Nongshim RedForce',
        'NS': 'Nongshim RedForce',
        '2Game Esports': '2Game Esports',
        'Apeks': 'Apeks',
    }
    
    # Team brand colors (same as team-race)
    team_colors = {
        'Paper Rex': '#FF69B4',           # Pink
        'FNATIC': '#FF6B00',              # Orange
        'Edward Gaming': '#FFFFFF',       # White
        'DRX': '#003366',                 # Dark Blue
        'Gen.G': '#FFD700',               # Gold
        'Team Heretics': '#FFFF00',       # Yellow
        'Sentinels': '#FF0000',           # Red
        'T1': '#DC143C',                  # Red (slightly darker crimson)
        'Team Liquid': '#0066CC',         # Blue
        'G2 Esports': '#FF6B6B',          # Light Red
        'NRG': '#FF8C00',                 # Orange (darker than FNATIC)
        'Leviatán': '#87CEEB',            # Light Blue
        'Bilibili Gaming': '#ADD8E6',     # Light Blue (slightly different)
        'LOUD': '#00FF00',                # Green
        'FUT Esports': '#808080',         # Grey
        'Rex Regum Qeon': '#DAA520',      # Gold (slightly darker than Gen.G)
        'Natus Vincere': '#FFD700',       # Yellow
        'Evil Geniuses': '#20B2AA',       # Blueish Green
        'Team Vitality': '#FFCC00',       # Yellow (slightly different)
        '100 Thieves': '#B22222',         # Red (firebrick)
        'KRÜ Esports': '#FF69B4',         # Pink
        'MIBR': '#4169E1',                # Blue
        'Cloud9': '#87CEEB',              # Light Blue
        'Karmine Corp': '#1E90FF',        # Blue
        'FunPlus Phoenix': '#FF4500',     # Red (orange-red)
        'Trace Esports': '#FFFFFF',       # White
        'Dragon Ranger Gaming': '#228B22',# Green (forest)
        'TALON': '#FF4444',               # Red (bright)
        'Team Secret': '#FFFFFF',         # White
        'BBL Esports': '#FFD700',         # Gold
        'ZETA DIVISION': '#FFFFFF',       # White
        'Giants': '#9370DB',              # Any - purple
        'FURIA': '#FF8C00',               # Any - orange
        'DetonatioN FocusMe': '#0000CD',  # Blue (medium)
        'TYLOO': '#8B0000',               # Dark Red
        'Global Esports': '#00CED1',      # Any - dark turquoise
        'KOI': '#4169E1',                 # Blue
        'Wolves Esports': '#FFD700',      # Gold
        'Nova Esports': '#800080',        # Purple
        'JDG Esports': '#FF0000',         # Red
        'Titan Esports Club': '#708090',  # Any - slate gray
        'Xi Lai Gaming': '#32CD32',       # Green
        'Gentle Mates': '#FFB6C1',        # Pink (light)
        'All Gamers': '#FF0000',          # Red
        'Nongshim RedForce': '#CC0000',   # Red
        'Giants Gaming': '#BA55D3',       # Any - medium orchid
        'BOOM Esports': '#8B0000',        # Dark Red
        'BLEED': '#9932CC',               # Any - dark orchid
        'Apeks': '#FFA500',               # Orange
        '2Game Esports': '#8B008B',       # Purple (dark magenta)
        # Additional teams
        'XSET': '#800080',                # Purple
        'OpTic Gaming': '#00FF00',        # Green
        'The Guard': '#FFD700',           # Gold
        'Version1': '#FF0000',            # Red
        'Gambit Esports': '#FF0000',      # Red
        'Acend': '#00FFFF',               # Cyan
        'G2 Gozen': '#FF69B4',            # Pink
        'Guild Esports': '#00FF00',       # Green
        'SuperMassive Blaze': '#FF4500',  # Orange Red
        'Oxygen Esports': '#00FFFF',      # Cyan
        'Attack All Around': '#FFD700',   # Gold
        'ONIC G': '#FFD700',              # Gold
        'Alter Ego': '#FF0000',           # Red
        'Rise': '#FFD700',                # Gold
    }
    
    def normalize_team_name(team_name):
        """Normalize team name by mapping to canonical name and extracting from parentheses."""
        if not team_name:
            return team_name
        
        # Check if it's in our mapping
        if team_name in team_name_mapping:
            return team_name_mapping[team_name]
        
        # Try to extract name from parentheses like "VISA KRÜ(KRÜ Esports)"
        match = re.search(r'\(([^)]+)\)', team_name)
        if match:
            extracted = match.group(1)
            if extracted in team_name_mapping:
                return team_name_mapping[extracted]
            return extracted
        
        return team_name
    
    # Tournament dates (Masters, Champions, LOCK//IN)
    tournaments = [
        # LOCK//IN event (green background)
        {'name': 'LOCK//IN 2023', 'start': '2023-02-13', 'end': '2023-03-04', 'type': 'lockin'},
        # Champions events (yellow background)
        {'name': 'Champions 2023', 'start': '2023-08-06', 'end': '2023-08-26', 'type': 'champions'},
        {'name': 'Champions 2024', 'start': '2024-08-01', 'end': '2024-08-25', 'type': 'champions'},
        {'name': 'Champions 2025', 'start': '2025-09-12', 'end': '2025-10-05', 'type': 'champions'},
        {'name': 'Champions 2026', 'start': '2026-09-23', 'end': '2026-10-18', 'type': 'champions'},
        # Masters events (purple background)
        {'name': 'Masters Tokyo 2023', 'start': '2023-06-10', 'end': '2023-06-25', 'type': 'masters'},
        {'name': 'Masters Madrid 2024', 'start': '2024-03-14', 'end': '2024-03-24', 'type': 'masters'},
        {'name': 'Masters Shanghai 2024', 'start': '2024-05-23', 'end': '2024-06-09', 'type': 'masters'},
        {'name': 'Masters Bangkok 2025', 'start': '2025-02-20', 'end': '2025-03-02', 'type': 'masters'},
        {'name': 'Masters Toronto 2025', 'start': '2025-06-07', 'end': '2025-06-22', 'type': 'masters'},
        {'name': 'Masters Santiago 2026', 'start': '2026-02-28', 'end': '2026-03-15', 'type': 'masters'},
        {'name': 'Masters London 2026', 'start': '2026-06-05', 'end': '2026-06-21', 'type': 'masters'},
    ]
    
    conn = database.get_db_connection()
    
    # Get top N players by maps played
    top_players_query = '''
        SELECT player, COUNT(*) as maps_played, SUM(kills) as total_kills, SUM(deaths) as total_deaths
        FROM matches 
        GROUP BY player 
        ORDER BY maps_played DESC 
        LIMIT ?
    '''
    top_players_rows = conn.execute(top_players_query, (track_n_players,)).fetchall()
    tracked_players = [row['player'] for row in top_players_rows]
    
    # Get all matches for tracked players, ordered by date (include team)
    placeholders = ','.join('?' * len(tracked_players))
    matches_query = f'''
        SELECT player, kills, deaths, match_date, team
        FROM matches 
        WHERE player IN ({placeholders})
        ORDER BY match_date
    '''
    rows = conn.execute(matches_query, tracked_players).fetchall()
    
    conn.close()
    
    if not rows:
        return jsonify({
            'dates': [],
            'players': [],
            'data': {},
            'kills_data': {},
            'player_dates': {},
            'tournaments': tournaments
        })
    
    # Process into date-based cumulative data
    # First, aggregate kills and deaths per player per date
    daily_stats = defaultdict(lambda: defaultdict(lambda: {'kills': 0, 'deaths': 0}))
    all_dates_set = set()
    
    # Track first and last appearance for each player
    player_first_date = {}
    player_last_date = {}
    # Track most recent team for each player (by date)
    player_recent_team = {}
    
    for row in rows:
        date = row['match_date']
        player = row['player']
        daily_stats[date][player]['kills'] += row['kills']
        daily_stats[date][player]['deaths'] += row['deaths']
        all_dates_set.add(date)
        
        # Track first appearance
        if player not in player_first_date or date < player_first_date[player]:
            player_first_date[player] = date
        # Track last appearance and most recent team
        if player not in player_last_date or date > player_last_date[player]:
            player_last_date[player] = date
            # Update most recent team when we see a newer date
            if row['team']:
                player_recent_team[player] = normalize_team_name(row['team'])
        elif player in player_last_date and date == player_last_date[player]:
            # Same date, still update team if available
            if row['team']:
                player_recent_team[player] = normalize_team_name(row['team'])
    
    all_dates = sorted(all_dates_set)
    
    # Create cumulative data (kills - deaths) and (total kills)
    player_kills = defaultdict(int)
    player_deaths = defaultdict(int)
    date_data = {}
    kills_data = {}
    
    for date in all_dates:
        # Update cumulative stats for players who played on this date
        for player in daily_stats[date]:
            player_kills[player] += daily_stats[date][player]['kills']
            player_deaths[player] += daily_stats[date][player]['deaths']
        
        # Store KD difference for each tracked player
        date_data[date] = {
            p: player_kills.get(p, 0) - player_deaths.get(p, 0) 
            for p in tracked_players
        }
        
        # Store total kills for each tracked player
        kills_data[date] = {
            p: player_kills.get(p, 0)
            for p in tracked_players
        }
    
    # Build response with player info including first/last dates and team color
    players_info = []
    for row in top_players_rows:
        kd = (row['total_kills'] or 0) - (row['total_deaths'] or 0)
        player_team = player_recent_team.get(row['player'])
        player_color = team_colors.get(player_team) if player_team else None
        players_info.append({
            'player': row['player'],
            'maps_played': row['maps_played'],
            'kd': kd,
            'total_kills': row['total_kills'] or 0,
            'first_date': player_first_date.get(row['player']),
            'last_date': player_last_date.get(row['player']),
            'team': player_team,
            'color': player_color
        })
    
    return jsonify({
        'dates': all_dates,
        'players': players_info,
        'data': date_data,
        'kills_data': kills_data,
        'max_date': all_dates[-1] if all_dates else None,
        'tournaments': tournaments
    })


@app.route('/api/team-race')
def api_team_race():
    """API endpoint for team K-D race chart data - returns cumulative K-D over time for top teams."""
    from collections import defaultdict
    import re
    
    # Configuration
    min_maps_played = 4  # Minimum maps played to be included (filters out teams with 3 or less)
    
    # Team name mapping - maps alternate names to canonical name
    team_name_mapping = {
        'VISA KRÜ': 'KRÜ Esports',
        'VISA KRÜ(KRÜ Esports)': 'KRÜ Esports',
        'KRÜ Esports': 'KRÜ Esports',
        'LEVIATÁN': 'Leviatán',
        'LOUD': 'LOUD',
        'FURIA': 'FURIA',
        'FURIA Esports': 'FURIA',
        'MIBR': 'MIBR',
        'MIBR Esports': 'MIBR',
        '100 Thieves': '100 Thieves',
        '100T': '100 Thieves',
        'Cloud9': 'Cloud9',
        'C9': 'Cloud9',
        'Evil Geniuses': 'Evil Geniuses',
        'EG': 'Evil Geniuses',
        'G2 Esports': 'G2 Esports',
        'G2': 'G2 Esports',
        'NRG Esports': 'NRG',
        'NRG': 'NRG',
        'Sentinels': 'Sentinels',
        'SEN': 'Sentinels',
        'FNATIC': 'FNATIC',
        'FNC': 'FNATIC',
        'FUT Esports': 'FUT Esports',
        'FUT': 'FUT Esports',
        'Guild Esports': 'Guild Esports',
        'GUILD': 'Guild Esports',
        'Team Liquid': 'Team Liquid',
        'TL': 'Team Liquid',
        'Team Vitality': 'Team Vitality',
        'Vitality': 'Team Vitality',
        'NaVi': 'Natus Vincere',
        'Natus Vincere': 'Natus Vincere',
        'NAVI': 'Natus Vincere',
        'Fnatic': 'FNATIC',
        'Paper Rex': 'Paper Rex',
        'PRX': 'Paper Rex',
        'DRX': 'DRX',
        'DRX (V/S Gaming)': 'DRX',
        'Dragon Ranger Gaming': 'Dragon Ranger Gaming',
        'DRG': 'Dragon Ranger Gaming',
        'ZETA DIVISION': 'ZETA DIVISION',
        'ZETA': 'ZETA DIVISION',
        'DetonatioN FocusMe': 'DetonatioN FocusMe',
        'DFM': 'DetonatioN FocusMe',
        'T1': 'T1',
        'Gen.G': 'Gen.G',
        'GenG': 'Gen.G',
        'RRQ': 'Rex Regum Qeon',
        'Rex Regum Qeon': 'Rex Regum Qeon',
        'Talon Esports': 'TALON',
        'TALON': 'TALON',
        'TLN': 'TALON',
        'Team Secret': 'Team Secret',
        'TS': 'Team Secret',
        'Global Esports': 'Global Esports',
        'GE': 'Global Esports',
        'BLEED': 'BLEED',
        'Team BLEED': 'BLEED',
        'Edward Gaming': 'Edward Gaming',
        'EDG': 'Edward Gaming',
        'FunPlus Phoenix': 'FunPlus Phoenix',
        'FPX': 'FunPlus Phoenix',
        'Bilibili Gaming': 'Bilibili Gaming',
        'BG': 'Bilibili Gaming',
        'Trace Esports': 'Trace Esports',
        'TE': 'Trace Esports',
        'JDG Gaming': 'JDG Esports',
        'JDG Esports': 'JDG Esports',
        'JDG': 'JDG Esports',
        'Titan FC': 'Titan Esports Club',
        'Titan Esports Club': 'Titan Esports Club',
        'XLG': 'Xi Lai Gaming',
        'Xi Lai Gaming': 'Xi Lai Gaming',
        'Wolves Esports': 'Wolves Esports',
        'WO': 'Wolves Esports',
        'Nova Esports': 'Nova Esports',
        'NOVA': 'Nova Esports',
        'Attack All Around': 'Attack All Around',
        'AAA': 'Attack All Around',
        'ONIC G': 'ONIC G',
        'ONIC': 'ONIC G',
        'Alter Ego': 'Alter Ego',
        'AE': 'Alter Ego',
        'BOOM Esports': 'BOOM Esports',
        'BOOM': 'BOOM Esports',
        'Rise': 'Rise',
        'XSET': 'XSET',
        'The Guard': 'The Guard',
        'Guard': 'The Guard',
        'OpTic Gaming': 'OpTic Gaming',
        'OpTic': 'OpTic Gaming',
        'Optic': 'OpTic Gaming',
        'Version1': 'Version1',
        'V1': 'Version1',
        'Gambit Esports': 'Gambit Esports',
        'Gambit': 'Gambit Esports',
        'Masters Seoul': 'Masters Seoul',
        'Acend': 'Acend',
        'ACE': 'Acend',
        'SuperMassive Blaze': 'SuperMassive Blaze',
        'SMB': 'SuperMassive Blaze',
        'Oxygen Esports': 'Oxygen Esports',
        'OXG': 'Oxygen Esports',
        'G store in': 'G store in',
        'G2 Gozen': 'G2 Gozen',
        'GUILD EK': 'GUILD EK',
        'FOKUS': 'FOKUS',
        'FOKUS ME': 'FOKUS',
        'Heretics': 'Team Heretics',
        'Team Heretics': 'Team Heretics',
        'TH': 'Team Heretics',
        'KOI': 'KOI',
        'Giants': 'Giants',
        'GIANTX': 'Giants',
        'GiantX': 'Giants',
        'Giants Gaming': 'Giants',
        'Vodafone Giants': 'Giants',
        'BBL Esports': 'BBL Esports',
        'BBL': 'BBL Esports',
        'Fire Flux Esports': 'Fire Flux Esports',
        'Fire Flux': 'Fire Flux Esports',
        'S2V Esports': 'S2V Esports',
        'S2V': 'S2V Esports',
        'Case Esports': 'Case Esports',
        'CASE': 'Case Esports',
        'M3 Champions': 'M3 Champions',
        'M3C': 'M3 Champions',
        'LDLC OL': 'LDLC OL',
        'LDLC': 'LDLC OL',
        'Funplus Phoenix': 'FunPlus Phoenix',
        'Liquid': 'Team Liquid',
        'Vitality': 'Team Vitality',
        'Navi': 'Natus Vincere',
        'Secret': 'Team Secret',
        'Talon': 'TALON',
        'Rex Regum': 'Rex Regum Qeon',
        'Paper': 'Paper Rex',
        'DetonatioN': 'DetonatioN FocusMe',
        'Edward': 'Edward Gaming',
        'Bilibili': 'Bilibili Gaming',
        'Wolves': 'Wolves Esports',
        'TYLOO': 'TYLOO',
        'TyLoo': 'TYLOO',
        'Karmine Corp': 'Karmine Corp',
        'KC': 'Karmine Corp',
        'Gentle Mates': 'Gentle Mates',
        'All Gamers': 'All Gamers',
        'AG': 'All Gamers',
        'Nongshim RedForce': 'Nongshim RedForce',
        'NS': 'Nongshim RedForce',
        '2Game Esports': '2Game Esports',
        'Apeks': 'Apeks',
    }
    
    # Team brand colors with variations
    team_colors = {
        'Paper Rex': '#FF69B4',           # Pink
        'FNATIC': '#FF6B00',              # Orange
        'Edward Gaming': '#FFFFFF',       # White
        'DRX': '#003366',                 # Dark Blue
        'Gen.G': '#FFD700',               # Gold
        'Team Heretics': '#FFFF00',       # Yellow
        'Sentinels': '#FF0000',           # Red
        'T1': '#DC143C',                  # Red (slightly darker crimson)
        'Team Liquid': '#0066CC',         # Blue
        'G2 Esports': '#FF6B6B',          # Light Red
        'NRG': '#FF8C00',                 # Orange (darker than FNATIC)
        'Leviatán': '#87CEEB',            # Light Blue
        'Bilibili Gaming': '#ADD8E6',     # Light Blue (slightly different)
        'LOUD': '#00FF00',                # Green
        'FUT Esports': '#808080',         # Grey
        'Rex Regum Qeon': '#DAA520',      # Gold (slightly darker than Gen.G)
        'Natus Vincere': '#FFD700',       # Yellow
        'Evil Geniuses': '#20B2AA',       # Blueish Green
        'Team Vitality': '#FFCC00',       # Yellow (slightly different)
        '100 Thieves': '#B22222',         # Red (firebrick)
        'KRÜ Esports': '#FF69B4',         # Pink
        'MIBR': '#4169E1',                # Blue
        'Cloud9': '#87CEEB',              # Light Blue
        'Karmine Corp': '#1E90FF',        # Blue
        'FunPlus Phoenix': '#FF4500',     # Red (orange-red)
        'Trace Esports': '#FFFFFF',       # White
        'Dragon Ranger Gaming': '#228B22',# Green (forest)
        'TALON': '#FF4444',               # Red (bright)
        'Team Secret': '#FFFFFF',         # White
        'BBL Esports': '#FFD700',         # Gold
        'ZETA DIVISION': '#FFFFFF',       # White
        'Giants': '#9370DB',              # Any - purple
        'FURIA': '#FF8C00',               # Any - orange
        'DetonatioN FocusMe': '#0000CD',  # Blue (medium)
        'TYLOO': '#8B0000',               # Dark Red
        'Global Esports': '#00CED1',      # Any - dark turquoise
        'KOI': '#4169E1',                 # Blue
        'Wolves Esports': '#FFD700',      # Gold
        'Nova Esports': '#800080',        # Purple
        'JDG Esports': '#FF0000',         # Red
        'Titan Esports Club': '#708090',  # Any - slate gray
        'Xi Lai Gaming': '#32CD32',       # Green
        'Gentle Mates': '#FFB6C1',        # Pink (light)
        'All Gamers': '#FF0000',          # Red
        'Nongshim RedForce': '#CC0000',   # Red
        'Giants Gaming': '#BA55D3',       # Any - medium orchid
        'BOOM Esports': '#8B0000',        # Dark Red
        'BLEED': '#9932CC',               # Any - dark orchid
        'Apeks': '#FFA500',               # Orange
        '2Game Esports': '#8B008B',       # Purple (dark magenta)
        # Additional teams
        'XSET': '#800080',                # Purple
        'OpTic Gaming': '#00FF00',        # Green
        'The Guard': '#FFD700',           # Gold
        'Version1': '#FF0000',            # Red
        'Gambit Esports': '#FF0000',      # Red
        'Acend': '#00FFFF',               # Cyan
        'G2 Gozen': '#FF69B4',            # Pink
        'Guild Esports': '#00FF00',       # Green
        'SuperMassive Blaze': '#FF4500',  # Orange Red
        'Oxygen Esports': '#00FFFF',      # Cyan
        'Attack All Around': '#FFD700',   # Gold
        'ONIC G': '#FFD700',              # Gold
        'Alter Ego': '#FF0000',           # Red
        'Rise': '#FFD700',                # Gold
    }
    
    # Tournament dates (Masters, Champions, LOCK//IN)
    tournaments = [
        # LOCK//IN event (green background)
        {'name': 'LOCK//IN 2023', 'start': '2023-02-13', 'end': '2023-03-04', 'type': 'lockin'},
        # Champions events (yellow background)
        {'name': 'Champions 2023', 'start': '2023-08-06', 'end': '2023-08-26', 'type': 'champions'},
        {'name': 'Champions 2024', 'start': '2024-08-01', 'end': '2024-08-25', 'type': 'champions'},
        {'name': 'Champions 2025', 'start': '2025-09-12', 'end': '2025-10-05', 'type': 'champions'},
        {'name': 'Champions 2026', 'start': '2026-09-23', 'end': '2026-10-18', 'type': 'champions'},
        # Masters events (purple background)
        {'name': 'Masters Tokyo 2023', 'start': '2023-06-10', 'end': '2023-06-25', 'type': 'masters'},
        {'name': 'Masters Madrid 2024', 'start': '2024-03-14', 'end': '2024-03-24', 'type': 'masters'},
        {'name': 'Masters Shanghai 2024', 'start': '2024-05-23', 'end': '2024-06-09', 'type': 'masters'},
        {'name': 'Masters Bangkok 2025', 'start': '2025-02-20', 'end': '2025-03-02', 'type': 'masters'},
        {'name': 'Masters Toronto 2025', 'start': '2025-06-07', 'end': '2025-06-22', 'type': 'masters'},
        {'name': 'Masters Santiago 2026', 'start': '2026-02-28', 'end': '2026-03-15', 'type': 'masters'},
        {'name': 'Masters London 2026', 'start': '2026-06-05', 'end': '2026-06-21', 'type': 'masters'},
    ]
    
    def normalize_team_name(team_name):
        """Normalize team name by mapping to canonical name and extracting from parentheses."""
        if not team_name:
            return team_name
        
        # Check if it's in our mapping
        if team_name in team_name_mapping:
            return team_name_mapping[team_name]
        
        # Try to extract name from parentheses like "VISA KRÜ(KRÜ Esports)"
        match = re.search(r'\(([^)]+)\)', team_name)
        if match:
            extracted = match.group(1)
            if extracted in team_name_mapping:
                return team_name_mapping[extracted]
            return extracted
        
        return team_name
    
    conn = database.get_db_connection()
    
    # Get all matches with team info
    all_matches_query = '''
        SELECT team, kills, deaths, match_date
        FROM matches 
        WHERE team IS NOT NULL AND team != ''
        ORDER BY match_date
    '''
    rows = conn.execute(all_matches_query).fetchall()
    conn.close()
    
    if not rows:
        return jsonify({
            'dates': [],
            'teams': [],
            'data': {},
            'kills_data': {},
            'tournaments': tournaments,
            'team_colors': team_colors
        })
    
    # Normalize team names and aggregate
    normalized_matches = []
    for row in rows:
        normalized_team = normalize_team_name(row['team'])
        normalized_matches.append({
            'team': normalized_team,
            'kills': row['kills'],
            'deaths': row['deaths'],
            'match_date': row['match_date']
        })
    
    # Count maps per normalized team
    team_maps = defaultdict(int)
    for match in normalized_matches:
        team_maps[match['team']] += 1
    
    # Showmatch/exhibition teams to exclude (these are not real competitive teams)
    showmatch_teams = {
        'Team tarik',
        'Team Alpha',
        'Team EMEA',
        'Team SuperBusS',
        'Team FRTTT',
        'Team Toast',
        'Glory Once Again',
        'Team Omega',
        'Team France',
        'Team World',
        'Team Bunny',
    }
    
    # Filter teams with minimum maps played AND not in showmatch teams
    valid_teams = {
        team for team, count in team_maps.items() 
        if count >= min_maps_played and team not in showmatch_teams
    }
    
    # Process into date-based cumulative data
    daily_stats = defaultdict(lambda: defaultdict(lambda: {'kills': 0, 'deaths': 0}))
    all_dates_set = set()
    
    # Track first and last appearance for each team
    team_first_date = {}
    team_last_date = {}
    
    for match in normalized_matches:
        team = match['team']
        if team not in valid_teams:
            continue
            
        date = match['match_date']
        daily_stats[date][team]['kills'] += match['kills']
        daily_stats[date][team]['deaths'] += match['deaths']
        all_dates_set.add(date)
        
        # Track first appearance
        if team not in team_first_date or date < team_first_date[team]:
            team_first_date[team] = date
        # Track last appearance
        if team not in team_last_date or date > team_last_date[team]:
            team_last_date[team] = date
    
    all_dates = sorted(all_dates_set)
    
    if not all_dates:
        return jsonify({
            'dates': [],
            'teams': [],
            'data': {},
            'kills_data': {},
            'tournaments': tournaments,
            'team_colors': team_colors
        })
    
    # Create cumulative data (kills - deaths) and (total kills)
    team_kills = defaultdict(int)
    team_deaths = defaultdict(int)
    date_data = {}
    kills_data = {}
    
    for date in all_dates:
        # Update cumulative stats for teams who played on this date
        for team in daily_stats[date]:
            team_kills[team] += daily_stats[date][team]['kills']
            team_deaths[team] += daily_stats[date][team]['deaths']
        
        # Store KD difference for each valid team
        date_data[date] = {
            t: team_kills.get(t, 0) - team_deaths.get(t, 0) 
            for t in valid_teams
        }
        
        # Store total kills for each valid team
        kills_data[date] = {
            t: team_kills.get(t, 0)
            for t in valid_teams
        }
    
    # Build response with team info sorted by maps played
    teams_info = []
    for team in sorted(valid_teams, key=lambda t: team_maps[t], reverse=True):
        teams_info.append({
            'team': team,
            'maps_played': team_maps[team],
            'kd': team_kills.get(team, 0) - team_deaths.get(team, 0),
            'total_kills': team_kills.get(team, 0),
            'first_date': team_first_date.get(team),
            'last_date': team_last_date.get(team),
            'color': team_colors.get(team)
        })
    
    return jsonify({
        'dates': all_dates,
        'teams': teams_info,
        'data': date_data,
        'kills_data': kills_data,
        'max_date': all_dates[-1] if all_dates else None,
        'tournaments': tournaments,
        'team_colors': team_colors
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