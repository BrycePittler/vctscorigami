"""
VCT Scorigami Flask Application

This is the main Flask application for the VCT Scorigami tracker.
It includes both the original manual entry system and new automation endpoints.
"""

from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
import database
import sqlite3
import bcrypt
import os
import logging
from datetime import datetime
from functools import wraps

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(24))

# Password hash for admin access
# Generate a new hash with: python -c "import bcrypt; print(bcrypt.hashpw('your_password'.encode('utf-8'), bcrypt.gensalt()))"
PASSWORD_HASH = os.environ.get(
    'PASSWORD_HASH',
    b"$2b$12$UxfOKV7MadrhIWPhy1Sozu3r0fhwr8pgshjd9t08XhSEvA791fWZO"
)

# Known VCT tournament IDs for automation
VCT_TOURNAMENT_IDS = {
    # 2025 Tournaments
    "Valorant Champions 2025": 2283,
    # 2024 Tournaments  
    "Valorant Champions 2024": 1923,
    "Champions Tour 2024: Masters Shanghai": 1909,
    "Champions Tour 2024: Masters Madrid": 1868,
    # 2023 Tournaments
    "Valorant Champions 2023": 1530,
    "Champions Tour 2023: Masters Tokyo": 1492,
    "Champions Tour 2023: Lock-In Sao Paulo": 1352,
}

# Initialize database on startup
database.init_db()


def login_required(f):
    """Decorator to require authentication for admin routes."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'authenticated' not in session:
            flash('Please authenticate to access this page.')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function


def rank_leaderboard(leaderboard_data, score_key):
    """
    Assigns ranks to a leaderboard list, handling ties correctly.
    Players with the same score get the same rank.
    """
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


# =============================================================================
# PUBLIC ROUTES (Original functionality)
# =============================================================================

@app.route('/')
def index():
    """Main page with score grid and leaderboards."""
    selected_player = request.args.get('player', 'all')
    selected_tournament = request.args.get('tournament', 'all')
    
    scores = database.get_scores(
        selected_player if selected_player != 'all' else None,
        selected_tournament if selected_tournament != 'all' else None
    )
    
    max_count = max([info['count'] for info in scores.values()]) if scores else 1
    
    conn = database.get_db_connection()
    
    # Get overall scorigamis
    overall_scorigamis_raw = conn.execute('''
        SELECT kills, deaths
        FROM matches
        GROUP BY kills, deaths
        HAVING COUNT(*) = 1
    ''').fetchall()
    
    overall_scorigamis = {(s['kills'], s['deaths']) for s in overall_scorigamis_raw}
    
    unique_players = conn.execute('SELECT DISTINCT player FROM matches ORDER BY LOWER(player)').fetchall()
    unique_tournaments = database.get_unique_tournaments()
    
    # Leaderboard queries
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
    
    leaderboard_kd_raw = conn.execute('''
        SELECT player, SUM(kills) - SUM(deaths) AS kill_death_difference
        FROM matches
        GROUP BY player
        ORDER BY kill_death_difference DESC
    ''').fetchall()

    # Total kills/deaths
    query = 'SELECT SUM(kills) as total_kills, SUM(deaths) as total_deaths FROM matches'
    params = []
    conditions = []
    if selected_player != 'all':
        conditions.append('player = ?')
        params.append(selected_player)
    if selected_tournament != 'all':
        conditions.append('description LIKE ?')
        params.append(f'%{selected_tournament}%')
    if conditions:
        query += ' WHERE ' + ' AND '.join(conditions)
    totals = conn.execute(query, params).fetchone()
    total_kills = totals['total_kills'] if totals and totals['total_kills'] else 0
    total_deaths = totals['total_deaths'] if totals and totals['total_deaths'] else 0

    conn.close()
    
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
    """Manual match entry page (original functionality)."""
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
                           deaths='')


# =============================================================================
# AUTOMATION ROUTES (New functionality)
# =============================================================================

@app.route('/admin')
@login_required
def admin_dashboard():
    """Admin dashboard for automation controls."""
    stats = {
        'total_matches': database.get_total_matches(),
        'unique_players': len(database.get_unique_players()),
        'recent_matches': database.get_recent_matches(10),
        'available_tournaments': VCT_TOURNAMENT_IDS
    }
    return render_template('admin.html', stats=stats)


@app.route('/admin/fetch', methods=['POST'])
@login_required
def admin_fetch_tournament():
    """
    Fetch and import data from a specific tournament.
    
    JSON body:
        tournament_id: int - VLR.gg event ID
        tournament_name: str - Name for description field
    """
    try:
        from data_fetcher import fetch_and_prepare_for_database
        
        data = request.get_json()
        tournament_id = data.get('tournament_id')
        tournament_name = data.get('tournament_name', f'Tournament {tournament_id}')
        
        if not tournament_id:
            return jsonify({
                'success': False,
                'error': 'tournament_id is required'
            }), 400
        
        logger.info(f"Starting fetch for tournament {tournament_id}: {tournament_name}")
        
        # Fetch data
        matches = fetch_and_prepare_for_database(int(tournament_id))
        
        if not matches:
            return jsonify({
                'success': True,
                'message': 'No matches found for this tournament',
                'added': 0,
                'skipped': 0
            })
        
        # Update descriptions with proper tournament name
        for match in matches:
            match['description'] = tournament_name
        
        # Insert into database
        results = database.add_matches_batch(matches)
        
        logger.info(f"Fetch complete: {results}")
        
        return jsonify({
            'success': True,
            'message': f'Successfully processed {len(matches)} records',
            'added': results['added'],
            'skipped': results['skipped'],
            'failed': results['failed']
        })
        
    except ImportError:
        logger.error("vlrdevapi not installed")
        return jsonify({
            'success': False,
            'error': 'vlrdevapi not installed. Run: pip install vlrdevapi'
        }), 500
    except Exception as e:
        logger.error(f"Error in fetch: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/admin/fetch-all', methods=['POST'])
@login_required
def admin_fetch_all():
    """Fetch data from all known VCT tournaments."""
    try:
        from data_fetcher import fetch_and_prepare_for_database
        
        total_results = {'added': 0, 'skipped': 0, 'failed': 0}
        processed = []
        
        for name, tid in VCT_TOURNAMENT_IDS.items():
            try:
                logger.info(f"Fetching {name} (ID: {tid})")
                matches = fetch_and_prepare_for_database(tid)
                
                for match in matches:
                    match['description'] = name
                
                results = database.add_matches_batch(matches)
                total_results['added'] += results['added']
                total_results['skipped'] += results['skipped']
                total_results['failed'] += results['failed']
                processed.append({
                    'tournament': name,
                    'success': True,
                    'matches_found': len(matches),
                    'added': results['added']
                })
            except Exception as e:
                logger.error(f"Error fetching {name}: {e}")
                processed.append({
                    'tournament': name,
                    'success': False,
                    'error': str(e)
                })
        
        return jsonify({
            'success': True,
            'total': total_results,
            'details': processed
        })
        
    except ImportError:
        return jsonify({
            'success': False,
            'error': 'vlrdevapi not installed'
        }), 500
    except Exception as e:
        logger.error(f"Error in fetch-all: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/admin/stats')
@login_required
def admin_stats():
    """Get database statistics."""
    return jsonify({
        'total_matches': database.get_total_matches(),
        'unique_players': len(database.get_unique_players()),
        'leaderboards': database.get_leaderboard_data()
    })


@app.route('/admin/recent')
@login_required
def admin_recent():
    """Get recent matches."""
    limit = request.args.get('limit', 50, type=int)
    matches = database.get_recent_matches(limit)
    return jsonify(matches)


@app.route('/admin/delete/<int:match_id>', methods=['DELETE'])
@login_required
def admin_delete_match(match_id):
    """Delete a specific match record."""
    success = database.delete_match(match_id)
    return jsonify({
        'success': success,
        'message': 'Match deleted' if success else 'Failed to delete match'
    })


@app.route('/admin/logout')
def admin_logout():
    """Clear admin session."""
    session.pop('authenticated', None)
    flash('Logged out successfully.')
    return redirect(url_for('index'))


# =============================================================================
# UTILITY ROUTES
# =============================================================================

@app.route('/health')
def health_check():
    """Health check endpoint for monitoring."""
    try:
        total_matches = database.get_total_matches()
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'total_matches': total_matches
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500


@app.route('/api/tournaments')
def api_tournaments():
    """Get list of available tournaments for automation."""
    return jsonify(VCT_TOURNAMENT_IDS)


# =============================================================================
# ERROR HANDLERS
# =============================================================================

@app.errorhandler(404)
def not_found(error):
    return render_template('error.html', error='Page not found'), 404


@app.errorhandler(500)
def server_error(error):
    logger.error(f"Server error: {error}")
    return render_template('error.html', error='Internal server error'), 500


if __name__ == '__main__':
    app.run(debug=True)