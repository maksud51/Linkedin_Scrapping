"""
Utility helper functions
"""

import asyncio
import random
import hashlib
from datetime import datetime
from typing import Any, List
import logging

logger = logging.getLogger(__name__)


def generate_hash(text: str) -> str:
    """Generate MD5 hash"""
    return hashlib.md5(text.encode()).hexdigest()


def get_timestamp() -> str:
    """Get current timestamp"""
    return datetime.now().isoformat()


def format_time(seconds: float) -> str:
    """Format seconds to readable format"""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


def print_progress_bar(current: int, total: int, prefix: str = '', suffix: str = '', length: int = 50):
    """Print progress bar"""
    if total == 0:
        return
    
    percent = current / total
    filled = int(length * percent)
    bar = '█' * filled + '░' * (length - filled)
    
    print(f'\r{prefix} |{bar}| {percent*100:.1f}% {suffix}', end='')
    
    if current == total:
        print()


def retry_async(max_retries: int = 3, delay: float = 1.0):
    """Decorator for async retry logic"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"Retry {attempt + 1}/{max_retries}: {e}")
                    await asyncio.sleep(delay * (2 ** attempt))  # Exponential backoff
            return None
        return wrapper
    return decorator


def sanitize_filename(filename: str) -> str:
    """Sanitize filename for filesystem"""
    import re
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    return filename[:255]


def extract_url_profile_id(url: str) -> str:
    """Extract profile ID from LinkedIn URL"""
    import re
    match = re.search(r'/in/([^/?]+)', url)
    return match.group(1) if match else ''


def print_banner():
    """Print banner"""
    banner = """
    ===========================================================
    
             LinkedIn Bulk Profile Scraper v2.0
         Advanced Anti-Detection & Multi-Agent System
    
      Features:
        [OK] Human-like behavior & fingerprint spoofing
        [OK] Text-based data extraction (no HTML elements)
        [OK] Multi-agent architecture (Search, Scrape, Validate)
        [OK] Resume capability with SQLite progress tracking
        [OK] CAPTCHA detection & manual solving
        [OK] Adaptive rate limiting & IP rotation ready
        [OK] Export to JSON/CSV/Excel with statistics
    
      [!] DISCLAIMER: This tool violates LinkedIn ToS.
      [!] Use for educational purposes only!
    
    ===========================================================
    """
    print(banner)


def print_config_info(config):
    """Print configuration information"""
    print("\n" + "="*60)
    print("[CONFIG] CONFIGURATION")
    print("="*60)
    print(f"Headless Mode: {config.HEADLESS}")
    print(f"Max Profiles per Search: {config.scraping['max_profiles_per_search']}")
    print(f"Delay Between Profiles: {config.scraping['delay_between_profiles']}")
    print(f"Stealth Mode: {config.scraping['use_stealth']}")
    print(f"Human Behavior: {config.anti_detection['human_behavior']}")
    print(f"Database: {config.database['path']}")
    print(f"Export Formats: {', '.join(config.export['formats'])}")
    print("="*60 + "\n")
