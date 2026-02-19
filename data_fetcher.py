"""
VCT Scorigami Data Fetcher Module
Fetches match data from VLR.gg using web scraping.
Includes date and win/loss status for each player per map.
"""
import time
import logging
import re
from typing import List, Dict, Optional, Tuple
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# VLR.gg base URL
VLR_BASE_URL = "https://www.vlr.gg"

# Request headers to mimic browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
}


def fetch_page(url: str, retries: int = 3) -> Optional[str]:
    """Fetch a page with retry logic."""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    
    logger.error(f"Failed to fetch {url} after {retries} attempts")
    return None


def get_match_page_urls(tournament_id: int) -> List[str]:
    """Get all match page URLs for a tournament."""
    url = f"{VLR_BASE_URL}/event/matches/{tournament_id}"
    html = fetch_page(url)
    
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    matches = []
    
    for link in soup.find_all('a', href=True):
        href = link.get('href')
        if href and re.match(r'^/\d+/', href):
            if '/event/' in href:
                continue
            match_url = f"{VLR_BASE_URL}{href}"
            matches.append(match_url)
    
    unique_matches = list(set(matches))
    logger.info(f"Found {len(unique_matches)} unique matches for tournament {tournament_id}")
    return unique_matches


def extract_date_from_match_page(soup: BeautifulSoup) -> Optional[str]:
    """Extract match date from the match page."""
    date_div = soup.find('div', class_='moment-tz-convert')
    if date_div:
        utc_ts = date_div.get('data-utc-ts')
        if utc_ts:
            try:
                dt = datetime.strptime(utc_ts, '%Y-%m-%d %H:%M:%S')
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                pass
        
        date_str = date_div.get_text(strip=True)
        for fmt in ['%B %d, %Y', '%A, %B %d', '%B %d']:
            try:
                dt = datetime.strptime(date_str, fmt)
                if dt.year == 1900:
                    dt = dt.replace(year=datetime.now().year)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
    
    date_text = soup.find(string=re.compile(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}'))
    if date_text:
        try:
            dt = datetime.strptime(date_text.strip(), '%B %d, %Y')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass
    
    return None


def extract_kills_from_cell(cell) -> Optional[int]:
    """Extract total kills from a kills cell."""
    total_span = cell.find('span', class_='mod-both')
    if total_span:
        try:
            return int(total_span.get_text(strip=True))
        except ValueError:
            pass
    
    text = cell.get_text(strip=True)
    match = re.search(r'^\d+', text)
    if match:
        try:
            return int(match.group())
        except ValueError:
            pass
    
    return None

from bs4 import BeautifulSoup

def is_match_complete(html_content: str) -> bool:
    """Check if a VLR.gg match page is for a completed match."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Check for "LIVE" badge
    live_badge = soup.find(class_='ml-status')  # or 'match-live', check VLR's current HTML
    if live_badge and 'live' in live_badge.get_text().lower():
        return False
    
    # Check for winner display
    winner = soup.find(class_='match-winner')
    if not winner:
        return False
    
    return True

def extract_deaths_from_cell(cell) -> Optional[int]:
    """Extract total deaths from a deaths cell."""
    total_span = cell.find('span', class_='mod-both')
    if total_span:
        try:
            return int(total_span.get_text(strip=True))
        except ValueError:
            pass
    
    text = cell.get_text(strip=True)
    match = re.search(r'/\s*(\d+)', text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            pass
    
    return None


def parse_match_page(html: str, match_url: str) -> Tuple[List[Dict], str, Optional[str]]:
    """Parse a match page and extract player statistics for all maps."""
    soup = BeautifulSoup(html, 'html.parser')
    matches_data = []
    
    match_id = None
    match = re.search(r'/(\d+)/', match_url)
    if match:
        match_id = match.group(1)
    
    tournament_name = ""
    tournament_div = soup.find('div', class_='match-header-event')
    if tournament_div:
        tournament_text = tournament_div.get_text(strip=True)
        tournament_name = tournament_text.split('\n')[0].strip()
    
    match_date = extract_date_from_match_page(soup)
    
    team_names = []
    team_divs = soup.find_all('div', class_='match-header-link-name')
    for team_div in team_divs:
        team_link = team_div.find('a')
        if team_link:
            team_names.append(team_link.get_text(strip=True))
    
    if len(team_names) < 2:
        team_headers = soup.find_all('div', class_='wf-title-med')
        for th in team_headers[:2]:
            text = th.get_text(strip=True)
            if text and not text.isdigit():
                team_names.append(text)
    
    game_sections = soup.find_all('div', class_='vm-stats-game')
    
    for game in game_sections:
        map_div = game.find('div', class_='map')
        if not map_div:
            continue
        
        map_name = ""
        map_header = game.find('div', class_='map')
        if map_header:
            map_text = map_header.get_text(strip=True)
            map_name = re.sub(r'\d+:\d+.*', '', map_text).strip()
            map_name = re.sub(r'\s*PICK.*', '', map_name).strip()
        
        if not map_name:
            continue
        
        team_scores = []
        score_divs = game.find_all('div', class_='score')
        for score_div in score_divs:
            score_text = score_div.get_text(strip=True)
            try:
                score = int(score_text)
                team_scores.append(score)
            except ValueError:
                pass
        
        winning_team_idx = None
        if len(team_scores) >= 2:
            if team_scores[0] > team_scores[1]:
                winning_team_idx = 0
            elif team_scores[1] > team_scores[0]:
                winning_team_idx = 1
        
        tables = game.find_all('table', class_='wf-table-inset')
        
        for team_idx, table in enumerate(tables):
            team_name = team_names[team_idx] if team_idx < len(team_names) else f"Team {team_idx + 1}"
            
            if winning_team_idx is not None:
                result = "Win" if team_idx == winning_team_idx else "Loss"
            else:
                result = "Tie"
            
            tbody = table.find('tbody')
            if not tbody:
                continue
            
            rows = tbody.find_all('tr')
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 4:
                    continue
                
                player_cell = cells[0]
                player_div = player_cell.find('div', style=lambda x: x and 'font-weight: 700' in x)
                if player_div:
                    player_name = player_div.get_text(strip=True)
                else:
                    player_name = player_cell.get_text(strip=True)
                    player_name = re.sub(r'\s+', ' ', player_name).strip()
                
                if not player_name or player_name.isdigit():
                    continue
                
                kills_cell = row.find('td', class_='mod-vlr-kills')
                if not kills_cell:
                    continue
                
                kills = extract_kills_from_cell(kills_cell)
                if kills is None:
                    continue
                
                deaths_cell = row.find('td', class_='mod-vlr-deaths')
                if not deaths_cell:
                    continue
                
                deaths = extract_deaths_from_cell(deaths_cell)
                if deaths is None:
                    continue
                
                if len(team_names) >= 2:
                    description = f"{tournament_name} - {team_names[0]} vs {team_names[1]}"
                else:
                    description = tournament_name
                
                match_data = {
                    'description': description,
                    'map': map_name,
                    'player': player_name,
                    'kills': kills,
                    'deaths': deaths,
                    'match_date': match_date,
                    'result': result,
                    'team': team_name,
                    'match_id': match_id
                }
                
                matches_data.append(match_data)
    
    return matches_data, tournament_name, match_date


def fetch_tournament_data(tournament_id: int, delay: float = 1.0) -> List[Dict]:
    """Fetch all match data for a tournament."""
    logger.info(f"Fetching data for tournament {tournament_id}")
    
    match_urls = get_match_page_urls(tournament_id)
    
    all_matches = []
    tournament_name = ""
    
    for i, match_url in enumerate(match_urls):
        logger.info(f"Processing match {i + 1}/{len(match_urls)}: {match_url}")
        
        html = fetch_page(match_url)
        if not html:
            continue
        
        matches, tourn_name, _ = parse_match_page(html, match_url)
        
        if matches:
            for match in matches:
                match['tournament_id'] = tournament_id
            
            all_matches.extend(matches)
            if not tournament_name:
                tournament_name = tourn_name
        
        if delay > 0:
            time.sleep(delay)
    
    logger.info(f"Extracted {len(all_matches)} player-map records from tournament {tournament_id}")
    return all_matches


def fetch_all_tier1_data(tournament_ids: List[int] = None, delay: float = 1.0) -> List[Dict]:
    """Fetch data for all tier 1 tournaments."""
    from tournament_discovery import TIER1_TOURNAMENT_IDS
    
    if tournament_ids is None:
        tournament_ids = TIER1_TOURNAMENT_IDS
    
    all_data = []
    
    logger.info(f"Fetching data for {len(tournament_ids)} tournaments")
    
    for i, tid in enumerate(tournament_ids):
        logger.info(f"\n{'='*50}")
        logger.info(f"Tournament {i + 1}/{len(tournament_ids)}: ID {tid}")
        logger.info(f"{'='*50}")
        
        matches = fetch_tournament_data(tid, delay)
        all_data.extend(matches)
        
        total_kills = sum(m['kills'] for m in matches)
        total_deaths = sum(m['deaths'] for m in matches)
        logger.info(f"Tournament {tid} summary: {len(matches)} records, K/D balance: {total_kills - total_deaths}")
    
    logger.info(f"\n{'='*50}")
    logger.info(f"FINAL SUMMARY")
    logger.info(f"{'='*50}")
    total_kills = sum(m['kills'] for m in all_data)
    total_deaths = sum(m['deaths'] for m in all_data)
    logger.info(f"Total records: {len(all_data)}")
    logger.info(f"Total kills: {total_kills}")
    logger.info(f"Total deaths: {total_deaths}")
    logger.info(f"K/D balance: {total_kills - total_deaths}")
    
    return all_data


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        tid = int(sys.argv[1])
    else:
        tid = 1923
    
    print(f"Testing fetch for tournament {tid}")
    matches = fetch_tournament_data(tid)
    
    print(f"\nExtracted {len(matches)} records")
    if matches:
        print("\nSample records:")
        for m in matches[:5]:
            print(f"  {m['player']}: {m['kills']}/{m['deaths']} on {m['map']} ({m.get('result', 'N/A')}) - {m.get('match_date', 'N/A')}")