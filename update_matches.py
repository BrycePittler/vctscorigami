"""
VCT Scorigami Auto-Updater
Fetches new matches from ongoing/recent tournaments.
Automatically detects active tournaments from VLR.gg.
"""
import logging
import sys
from datetime import datetime, timedelta

import database
from data_fetcher import fetch_tournament_data
from tournament_discovery import TIER1_TOURNAMENT_IDS, discover_all_tier1_tournaments

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('update.log')
    ]
)
logger = logging.getLogger(__name__)


def get_active_tournament_ids(days_back=14):
    """
    Dynamically detect active tournaments from VLR.gg.
    
    A tournament is considered "active" if:
    1. It has matches scheduled/played in the last X days, OR
    2. It's marked as ongoing on VLR.gg
    
    Args:
        days_back: Number of days to look back for recent activity
    
    Returns:
        List of tournament IDs to check
    """
    logger.info("Detecting active tournaments from VLR.gg...")
    
    # Get all tier 1 tournaments
    all_tournaments = discover_all_tier1_tournaments()
    tournament_ids = [int(tid) for tid in all_tournaments.keys()]
    
    # Get tournaments that have recent matches in our database
    conn = database.get_db_connection()
    cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    
    if database.USE_POSTGRES:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT DISTINCT tournament_id FROM matches WHERE match_date >= %s AND tournament_id IS NOT NULL',
            (cutoff_date,)
        )
        recent_tournament_ids = [row[0] for row in cursor.fetchall()]
    else:
        cursor = conn.execute(
            'SELECT DISTINCT tournament_id FROM matches WHERE match_date >= ? AND tournament_id IS NOT NULL',
            (cutoff_date,)
        )
        recent_tournament_ids = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    # Combine discovered tournaments with ones that have recent activity
    active_ids = list(set(tournament_ids + recent_tournament_ids))
    
    logger.info(f"Found {len(active_ids)} tournaments to check")
    return active_ids


def get_recent_and_upcoming_tournament_ids():
    """
    Get tournaments that are likely to have recent or upcoming matches.
    Uses current year + previous year to be safe.
    """
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    # If early in year (Jan-Mar), include previous year's tournaments too
    years_to_check = [current_year]
    if current_month <= 3:
        years_to_check.append(current_year - 1)
    
    logger.info(f"Checking tournaments from years: {years_to_check}")
    
    # Get all tournaments and filter by year
    all_tournaments = discover_all_tier1_tournaments()
    
    active_ids = []
    for tid_str, name in all_tournaments.items():
        tid = int(tid_str)
        # Check if tournament name contains current year or is in our known list
        name_upper = name.upper()
        if any(str(year) in name_upper for year in years_to_check):
            active_ids.append(tid)
        elif any(str(year) in name for year in years_to_check):
            active_ids.append(tid)
    
    # Also include tournaments that have had matches in last 30 days
    conn = database.get_db_connection()
    cutoff_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    if database.USE_POSTGRES:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT DISTINCT tournament_id FROM matches WHERE match_date >= %s AND tournament_id IS NOT NULL',
            (cutoff_date,)
        )
    else:
        cursor = conn.execute(
            'SELECT DISTINCT tournament_id FROM matches WHERE match_date >= ? AND tournament_id IS NOT NULL',
            (cutoff_date,)
        )
    
    for row in cursor.fetchall():
        tid = row[0]
        if tid not in active_ids:
            active_ids.append(tid)
    
    conn.close()
    
    logger.info(f"Found {len(active_ids)} active/recent tournaments")
    return active_ids


def update_matches(tournament_ids=None, delay=0.5):
    """Fetch new matches for specified tournaments."""
    logger.info("="*60)
    logger.info(f"VCT Scorigami Update Started: {datetime.now()}")
    logger.info("="*60)
    
    database.init_db()
    
    stats_before = database.get_database_stats()
    logger.info(f"Database before: {stats_before['total_matches']} matches")
    
    if tournament_ids is None:
        tournament_ids = get_recent_and_upcoming_tournament_ids()
    
    logger.info(f"Checking {len(tournament_ids)} tournaments")
    
    total_new = 0
    total_skipped = 0
    errors = []
    
    for tid in tournament_ids:
        logger.info(f"\nChecking tournament {tid}...")
        
        try:
            matches = fetch_tournament_data(tid, delay=delay)
            
            if matches:
                inserted, skipped = database.add_matches_batch(matches)
                total_new += inserted
                total_skipped += skipped
                
                if inserted > 0:
                    logger.info(f"  {inserted} NEW matches added!")
                else:
                    logger.info(f"  No new matches (all duplicates)")
            else:
                logger.info(f"  No matches found (tournament may be upcoming or live)")
                
        except Exception as e:
            logger.error(f"  Error: {e}")
            errors.append((tid, str(e)))
            continue
    
    stats_after = database.get_database_stats()
    
    logger.info("\n" + "="*60)
    logger.info("UPDATE COMPLETE")
    logger.info("="*60)
    logger.info(f"New matches added: {total_new}")
    logger.info(f"Skipped (duplicates): {total_skipped}")
    logger.info(f"Database after: {stats_after['total_matches']} matches")
    
    return total_new


def update_all_tier1(delay=0.5):
    """Update ALL tier 1 tournaments."""
    logger.info("Running FULL update...")
    return update_matches(tournament_ids=TIER1_TOURNAMENT_IDS, delay=delay)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='VCT Scorigami Auto-Updater')
    parser.add_argument('--all', '-a', action='store_true',
                        help='Update ALL tier 1 tournaments')
    parser.add_argument('--tournaments', '-t', type=int, nargs='+',
                        help='Specific tournament IDs to update')
    parser.add_argument('--delay', '-d', type=float, default=0.5,
                        help='Delay between requests')
    
    args = parser.parse_args()
    
    if args.all:
        update_all_tier1(delay=args.delay)
    elif args.tournaments:
        update_matches(tournament_ids=args.tournaments, delay=args.delay)
    else:
        update_matches(delay=args.delay)