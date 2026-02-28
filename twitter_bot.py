"""
VCT Scorigami Twitter Bot
Automatically tweets when new unique scorelines (scorigamis) occur.
"""
import os
import logging
import tweepy
from typing import List, Dict, Optional
import database

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Twitter API credentials - loaded from environment variables
# NEVER hardcode these in your script!
API_KEY = os.environ.get('TWITTER_API_KEY')
API_KEY_SECRET = os.environ.get('TWITTER_API_KEY_SECRET')
ACCESS_TOKEN = os.environ.get('TWITTER_ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.environ.get('TWITTER_ACCESS_TOKEN_SECRET')


class ScorigamiTwitterBot:
    """Twitter bot for posting VCT Scorigami updates."""
    
    def __init__(self):
        """Initialize the Twitter API client."""
        self.api = None
        self.client = None
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with Twitter API."""
        if not all([API_KEY, API_KEY_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET]):
            logger.error("Missing Twitter API credentials! Set environment variables.")
            return False
        
        try:
            # OAuth 1.0a authentication (required for posting tweets)
            auth = tweepy.OAuth1UserHandler(
                API_KEY, API_KEY_SECRET,
                ACCESS_TOKEN, ACCESS_TOKEN_SECRET
            )
            self.api = tweepy.API(auth)
            
            # OAuth 2.0 client for v2 API (optional, for newer features)
            self.client = tweepy.Client(
                consumer_key=API_KEY,
                consumer_secret=API_KEY_SECRET,
                access_token=ACCESS_TOKEN,
                access_token_secret=ACCESS_TOKEN_SECRET
            )
            
            # Verify credentials
            user = self.api.verify_credentials()
            logger.info(f"Authenticated as @{user.screen_name}")
            return True
            
        except Exception as e:
            logger.error(f"Twitter authentication failed: {e}")
            return False
    
    def is_authenticated(self) -> bool:
        """Check if bot is properly authenticated."""
        return self.api is not None
    
    def format_scorigami_tweet(self, scorigami: Dict) -> str:
        """
        Format a scorigami into a tweet.
        
        Args:
            scorigami: Dict with keys: kills, deaths, player, map, team, 
                      opponent, result, match_date, description
        
        Returns:
            Formatted tweet string (max 280 characters)
        """
        # Build the tweet
        kd_str = f"{scorigami['kills']}/{scorigami['deaths']}"
        player = scorigami['player']
        map_name = scorigami['map']
        team = scorigami.get('team', '')
        opponent = scorigami.get('opponent', '')
        result = scorigami.get('result', '')
        
        # Format the match context
        if team and opponent:
            match_context = f"{team} vs {opponent}"
        elif team:
            match_context = team
        else:
            match_context = ""
        
        # Build tweet
        parts = [f"Scorigami!"]
        parts.append(f"{player} went {kd_str} on {map_name}")
        
        if match_context:
            parts.append(f"({match_context} via: vctscorigami.com)")
        
        if result:
            emoji = "✅" if result == "Win" else "❌"
            parts.append(f"{emoji} {result}")
        
        parts.append("#vct #valorant #vctscorigami")
        
        tweet = " ".join(parts)
        
        # Ensure tweet is under 280 characters
        if len(tweet) > 280:
            # Shorten by removing some details
            tweet = f"Scorigami!\n{player}: {kd_str} on {map_name}\n#vct #valorant #vctscorigami"
        
        return tweet
    
    def post_tweet(self, text: str) -> Optional[int]:
        """
        Post a tweet.
        
        Args:
            text: The tweet text (max 280 characters)
        
        Returns:
            Tweet ID if successful, None otherwise
        """
        if not self.is_authenticated():
            logger.error("Cannot post tweet: Not authenticated")
            return None
        
        if len(text) > 280:
            logger.warning(f"Tweet too long ({len(text)} chars), truncating")
            text = text[:277] + "..."
        
        try:
            # Using API v2 (recommended)
            response = self.client.create_tweet(text=text)
            tweet_id = response.data['id']
            logger.info(f"Tweet posted successfully! ID: {tweet_id}")
            return tweet_id
            
        except tweepy.TooManyRequests:
            logger.error("Rate limited! Wait before posting more tweets.")
            return None
        except tweepy.Forbidden:
            logger.error("Forbidden: Check your app permissions (needs Write access)")
            return None
        except Exception as e:
            logger.error(f"Failed to post tweet: {e}")
            return None
    
    def post_scorigami(self, scorigami: Dict) -> Optional[int]:
        """
        Post a tweet about a new scorigami.
        
        Args:
            scorigami: Dict with scorigami details
        
        Returns:
            Tweet ID if successful, None otherwise
        """
        tweet_text = self.format_scorigami_tweet(scorigami)
        logger.info(f"Posting scorigami tweet: {tweet_text[:50]}...")
        return self.post_tweet(tweet_text)


def get_existing_scorigamis() -> set:
    """
    Get all current scorigamis (K/D combos that occurred exactly once).
    
    Returns:
        Set of tuples (kills, deaths)
    """
    conn = database.get_db_connection()
    cursor = conn.execute('''
        SELECT kills, deaths
        FROM matches
        GROUP BY kills, deaths
        HAVING COUNT(*) = 1
    ''')
    scorigamis = {(row['kills'], row['deaths']) for row in cursor.fetchall()}
    conn.close()
    return scorigamis


def get_scorigami_details(kills: int, deaths: int) -> Optional[Dict]:
    """
    Get the match details for a specific scorigami.
    
    Args:
        kills: Number of kills
        deaths: Number of deaths
    
    Returns:
        Dict with match details or None if not found
    """
    conn = database.get_db_connection()
    
    # Get unique teams list for opponent matching
    unique_teams_raw = conn.execute(
        'SELECT DISTINCT team FROM matches WHERE team IS NOT NULL AND team != ""'
    ).fetchall()
    unique_teams = [row['team'] for row in unique_teams_raw]
    
    # Get the scorigami match
    cursor = conn.execute('''
        SELECT kills, deaths, player, map, team, result, match_date, description
        FROM matches
        WHERE kills = ? AND deaths = ?
    ''', (kills, deaths))
    
    row = cursor.fetchone()
    if not row:
        conn.close()
        return None
    
    # Extract opponent from description
    opponent = None
    if row['description'] and row['team']:
        desc = row['description']
        if ' vs ' in desc:
            parts = desc.split(' vs ')
            if len(parts) > 1:
                team2 = parts[1].strip()
                before_vs = parts[0].strip()
                
                # Determine opponent
                if row['team'] == team2:
                    words = before_vs.split()
                    for num_words in range(min(4, len(words)), 0, -1):
                        potential_team = ' '.join(words[-num_words:])
                        if potential_team in unique_teams and potential_team != row['team']:
                            opponent = potential_team
                            break
                else:
                    opponent = team2
    
    result = {
        'kills': row['kills'],
        'deaths': row['deaths'],
        'player': row['player'],
        'map': row['map'],
        'team': row['team'],
        'opponent': opponent,
        'result': row['result'],
        'match_date': row['match_date'],
        'description': row['description']
    }
    
    conn.close()
    return result


def get_posted_scorigamis() -> set:
    """
    Get scorigamis that have already been posted to Twitter.
    Uses a simple tracking table.
    
    Returns:
        Set of tuples (kills, deaths) that have been posted
    """
    conn = database.get_db_connection()
    
    # Create tracking table if it doesn't exist
    conn.execute('''
        CREATE TABLE IF NOT EXISTS posted_scorigamis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kills INTEGER NOT NULL,
            deaths INTEGER NOT NULL,
            tweet_id TEXT,
            posted_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(kills, deaths)
        )
    ''')
    conn.commit()
    
    cursor = conn.execute('SELECT kills, deaths FROM posted_scorigamis')
    posted = {(row['kills'], row['deaths']) for row in cursor.fetchall()}
    conn.close()
    return posted


def mark_scorigami_posted(kills: int, deaths: int, tweet_id: str = None):
    """
    Mark a scorigami as posted to Twitter.
    
    Args:
        kills: Number of kills
        deaths: Number of deaths
        tweet_id: The Twitter tweet ID (optional)
    """
    conn = database.get_db_connection()
    conn.execute('''
        INSERT OR IGNORE INTO posted_scorigamis (kills, deaths, tweet_id)
        VALUES (?, ?, ?)
    ''', (kills, deaths, tweet_id))
    conn.commit()
    conn.close()


def check_and_post_new_scorigamis(dry_run: bool = False) -> List[Dict]:
    """
    Check for new scorigamis and post them to Twitter.
    
    Args:
        dry_run: If True, don't actually post tweets
    
    Returns:
        List of new scorigamis that were found
    """
    logger.info("Checking for new scorigamis...")
    
    # Get current scorigamis and already posted ones
    current_scorigamis = get_existing_scorigamis()
    posted_scorigamis = get_posted_scorigamis()
    
    # Find new scorigamis (current - posted)
    new_scorigamis = current_scorigamis - posted_scorigamis
    
    if not new_scorigamis:
        logger.info("No new scorigamis found")
        return []
    
    logger.info(f"Found {len(new_scorigamis)} new scorigami(s)!")
    
    # Initialize bot
    bot = ScorigamiTwitterBot()
    
    posted = []
    for kd in sorted(new_scorigamis):
        kills, deaths = kd
        
        # Get details
        details = get_scorigami_details(kills, deaths)
        if not details:
            logger.warning(f"Could not get details for {kills}/{deaths}")
            continue
        
        logger.info(f"  {details['player']}: {kills}/{deaths} on {details['map']}")
        
        if dry_run:
            logger.info(f"  [DRY RUN] Would post: {bot.format_scorigami_tweet(details)[:50]}...")
            posted.append(details)
        else:
            # Post tweet
            tweet_id = bot.post_scorigami(details)
            if tweet_id:
                mark_scorigami_posted(kills, deaths, str(tweet_id))
                posted.append(details)
            else:
                logger.error(f"Failed to post scorigami {kills}/{deaths}")
    
    return posted


def main():
    """Main entry point for the Twitter bot."""
    import argparse
    
    parser = argparse.ArgumentParser(description='VCT Scorigami Twitter Bot')
    parser.add_argument('--dry-run', '-n', action='store_true',
                        help="Check for new scorigamis without posting")
    parser.add_argument('--test', action='store_true',
                        help="Test Twitter authentication")
    
    args = parser.parse_args()
    
    # Initialize database
    database.init_db()
    
    if args.test:
        bot = ScorigamiTwitterBot()
        if bot.is_authenticated():
            print("✅ Twitter authentication successful!")
        else:
            print("❌ Twitter authentication failed. Check your credentials.")
        return
    
    # Check and post new scorigamis
    new_scorigamis = check_and_post_new_scorigamis(dry_run=args.dry_run)
    
    if new_scorigamis:
        print(f"\n{'='*50}")
        print(f"Posted {len(new_scorigamis)} new scorigami(s)")
        print(f"{'='*50}")
        for s in new_scorigamis:
            print(f"  {s['player']}: {s['kills']}/{s['deaths']} on {s['map']}")
    else:
        print("No new scorigamis to post.")


if __name__ == '__main__':
    main()