"""
VCT Tournament Discovery Module
Automatically discovers all tier 1 tournament IDs from VLR.gg
"""
import requests
from bs4 import BeautifulSoup
import re
import logging
from typing import Dict, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# VLR.gg base URL
VLR_BASE_URL = "https://www.vlr.gg"

# Franchise era years (2023-2026)
FRANCHISE_YEARS = ['2023', '2024', '2025', '2026']

def get_tournaments_from_vct_page(year: str) -> Dict[str, str]:
    """
    Get all tier 1 tournaments from a VCT year page.
    
    Args:
        year: The year to fetch tournaments for (e.g., '2024')
    
    Returns:
        Dictionary mapping tournament ID to tournament name
    """
    url = f"{VLR_BASE_URL}/vct-{year}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        tournaments = {}
        events = soup.find_all('a', href=lambda x: x and '/event/' in x)
        
        for event in events:
            href = event.get('href')
            match = re.search(r'/event/(\d+)/', href)
            if match:
                event_id = match.group(1)
                text = event.get_text(strip=True)
                # Clean up text - remove status suffix
                text = re.sub(r'(completed|ongoing|upcoming)Status.*', '', text)
                tournaments[event_id] = text.strip()
        
        logger.info(f"Found {len(tournaments)} tournaments for VCT {year}")
        return tournaments
        
    except Exception as e:
        logger.error(f"Error fetching VCT {year}: {e}")
        return {}


def discover_all_tier1_tournaments() -> Dict[str, str]:
    """
    Discover all tier 1 tournaments from the franchise era (2023-2026).
    
    Returns:
        Dictionary mapping tournament ID to tournament name
    """
    all_tournaments = {}
    
    for year in FRANCHISE_YEARS:
        tournaments = get_tournaments_from_vct_page(year)
        all_tournaments.update(tournaments)
    
    logger.info(f"Total tier 1 tournaments discovered: {len(all_tournaments)}")
    return all_tournaments


def get_tournament_ids() -> List[int]:
    """
    Get list of all tier 1 tournament IDs.
    
    Returns:
        List of tournament IDs as integers
    """
    tournaments = discover_all_tier1_tournaments()
    return [int(tid) for tid in tournaments.keys()]


# Pre-defined list of all known tier 1 tournaments (as backup)
# Total: 49 tournaments across 2023-2026
TIER1_TOURNAMENT_IDS = [
    # ============ 2023 (10 tournaments) ============
    1188,  # Champions Tour 2023: LOCK//IN São Paulo
    1189,  # Champions Tour 2023: Americas League
    1190,  # Champions Tour 2023: EMEA League
    1191,  # Champions Tour 2023: Pacific League
    1494,  # Champions Tour 2023: Masters Tokyo
    1657,  # Valorant Champions 2023
    1658,  # Champions Tour 2023: Americas Last Chance Qualifier
    1659,  # Champions Tour 2023: EMEA Last Chance Qualifier
    1660,  # Champions Tour 2023: Pacific Last Chance Qualifier
    1664,  # Champions Tour 2023: Champions China Qualifier
    
    # ============ 2024 (15 tournaments) ============
    1921,  # Champions Tour 2024: Masters Madrid
    1923,  # Champions Tour 2024: Americas Kickoff
    1924,  # Champions Tour 2024: Pacific Kickoff
    1925,  # Champions Tour 2024: EMEA Kickoff
    1926,  # Champions Tour 2024: China Kickoff
    1998,  # Champions Tour 2024: EMEA Stage 1
    1999,  # Champions Tour 2024: Masters Shanghai
    2002,  # Champions Tour 2024: Pacific Stage 1
    2004,  # Champions Tour 2024: Americas Stage 1
    2005,  # Champions Tour 2024: Pacific Stage 2
    2006,  # Champions Tour 2024: China Stage 1
    2094,  # Champions Tour 2024: EMEA Stage 2
    2095,  # Champions Tour 2024: Americas Stage 2
    2096,  # Champions Tour 2024: China Stage 2
    2097,  # Valorant Champions 2024
    
    # ============ 2025 (15 tournaments) ============
    2274,  # VCT 2025: Americas Kickoff
    2275,  # VCT 2025: China Kickoff
    2276,  # VCT 2025: EMEA Kickoff
    2277,  # VCT 2025: Pacific Kickoff
    2281,  # Valorant Masters Bangkok 2025
    2282,  # Valorant Masters Toronto 2025
    2283,  # Valorant Champions 2025
    2347,  # VCT 2025: Americas Stage 1
    2359,  # VCT 2025: China Stage 1
    2379,  # VCT 2025: Pacific Stage 1
    2380,  # VCT 2025: EMEA Stage 1
    2498,  # VCT 2025: EMEA Stage 2
    2499,  # VCT 2025: China Stage 2
    2500,  # VCT 2025: Pacific Stage 2
    2501,  # VCT 2025: Americas Stage 2
    
    # ============ 2026 (9 tournaments - so far) ============
    2682,  # VCT 2026: Americas Kickoff
    2683,  # VCT 2026: Pacific Kickoff
    2684,  # VCT 2026: EMEA Kickoff
    2685,  # VCT 2026: China Kickoff
    2760,  # Valorant Masters Santiago 2026
    2765,  # Valorant Masters London 2026
    2766,  # Valorant Champions 2026
    2775,  # VCT 2026: Pacific Stage 1
    2776,  # VCT 2026: Pacific Stage 2
]


if __name__ == '__main__':
    # Test discovery
    tournaments = discover_all_tier1_tournaments()
    print(f"\nDiscovered {len(tournaments)} tier 1 tournaments:")
    for tid, name in sorted(tournaments.items(), key=lambda x: x[1]):
        print(f"  {tid}: {name}")