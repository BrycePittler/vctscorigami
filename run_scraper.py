"""
VCT Scorigami Scraper Runner
Orchestrates the scraping of all tier 1 VCT tournaments.
"""
import argparse
import logging
import sys
from datetime import datetime

import database
from data_fetcher import fetch_tournament_data, fetch_all_tier1_data
from tournament_discovery import TIER1_TOURNAMENT_IDS, discover_all_tier1_tournaments

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('scraper.log')
    ]
)
logger = logging.getLogger(__name__)


def run_scraper(tournament_ids=None, delay=1.0, dry_run=False):
    """
    Run the scraper for specified tournaments.
    
    Args:
        tournament_ids: List of tournament IDs to scrape. If None, scrapes all tier 1.
        delay: Delay between requests in seconds
        dry_run: If True, don't save to database
    """
    # Initialize database
    database.init_db()
    
    # Get current stats
    stats_before = database.get_database_stats()
    logger.info(f"Database stats before: {stats_before}")
    
    # Fetch data
    if tournament_ids:
        all_matches = []
        for tid in tournament_ids:
            matches = fetch_tournament_data(tid, delay)
            all_matches.extend(matches)
    else:
        all_matches = fetch_all_tier1_data(delay=delay)
    
    if dry_run:
        logger.info(f"DRY RUN: Would insert {len(all_matches)} records")
        return all_matches
    
    # Save to database
    logger.info(f"Inserting {len(all_matches)} records into database...")
    inserted, skipped = database.add_matches_batch(all_matches)
    
    logger.info(f"Inserted: {inserted}, Skipped (duplicates): {skipped}")
    
    # Verify data integrity
    kd_balance = database.verify_kill_death_balance()
    logger.info(f"Kill/Death balance: {kd_balance} (should be 0)")
    
    # Get new stats
    stats_after = database.get_database_stats()
    logger.info(f"Database stats after: {stats_after}")
    
    return all_matches


def main():
    parser = argparse.ArgumentParser(description='VCT Scorigami Scraper')
    parser.add_argument('--tournament', '-t', type=int, nargs='+',
                        help='Tournament ID(s) to scrape')
    parser.add_argument('--all', '-a', action='store_true',
                        help='Scrape all tier 1 tournaments')
    parser.add_argument('--delay', '-d', type=float, default=1.0,
                        help='Delay between requests in seconds (default: 1.0)')
    parser.add_argument('--dry-run', '-n', action='store_true',
                        help='Run without saving to database')
    parser.add_argument('--list-tournaments', '-l', action='store_true',
                        help='List all tier 1 tournaments')
    parser.add_argument('--discover', action='store_true',
                        help='Discover tournaments from VLR.gg')
    
    args = parser.parse_args()
    
    if args.list_tournaments:
        print("\nAll Tier 1 Tournament IDs:")
        print("-" * 50)
        for tid in TIER1_TOURNAMENT_IDS:
            print(f"  {tid}")
        print(f"\nTotal: {len(TIER1_TOURNAMENT_IDS)} tournaments")
        return
    
    if args.discover:
        print("\nDiscovering tournaments from VLR.gg...")
        tournaments = discover_all_tier1_tournaments()
        print(f"\nDiscovered {len(tournaments)} tournaments:")
        print("-" * 50)
        for tid, name in sorted(tournaments.items(), key=lambda x: x[1]):
            print(f"  {tid}: {name}")
        return
    
    if args.all:
        logger.info("Scraping ALL tier 1 tournaments")
        run_scraper(delay=args.delay, dry_run=args.dry_run)
    elif args.tournament:
        logger.info(f"Scraping tournaments: {args.tournament}")
        run_scraper(tournament_ids=args.tournament, delay=args.delay, dry_run=args.dry_run)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()