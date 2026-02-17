"""
VCT Scorigami Auto-Updater
Fetches new matches from ongoing/recent tournaments.
Designed to be run periodically (e.g., every few hours).
"""
import logging
import sys
from datetime import datetime, timedelta

import database
from data_fetcher import fetch_tournament_data
from tournament_discovery import TIER1_TOURNAMENT_IDS

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


# Manually maintained list of currently active tournaments
# Update this when new tournaments start
ACTIVE_TOURNAMENT_IDS = [
    # 2026 Active Tournaments - Update as tournaments start/end
    2682,  # VCT 2026: Americas Kickoff
    2683,  # VCT 2026: Pacific Kickoff  
    2684,  # VCT 2026: EMEA Kickoff
    2685,  # VCT 2026: China Kickoff
    2760,  # Valorant Masters Santiago 2026
    2775,  # VCT 2026: Pacific Stage 1
]

# Recent completed tournaments to check (in case of late data updates)
RECENT_TOURNAMENT_IDS = []


def get_active_tournament_ids():
    """Get list of tournaments to check."""
    ids = list(ACTIVE_TOURNAMENT_IDS)
    ids.extend(RECENT_TOURNAMENT_IDS)
    return list(set(ids))


def update_matches(tournament_ids=None, delay=0.5):
    """Fetch new matches for specified tournaments."""
    logger.info("="*60)
    logger.info(f"VCT Scorigami Update Started: {datetime.now()}")
    logger.info("="*60)
    
    database.init_db()
    
    stats_before = database.get_database_stats()
    logger.info(f"Database before: {stats_before['total_matches']} matches")
    
    if tournament_ids is None:
        tournament_ids = get_active_tournament_ids()
    
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
                    logger.info(f"  No new matches")
            else:
                logger.info(f"  No matches found")
                
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