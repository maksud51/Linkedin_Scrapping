"""
Search Agent: Finds LinkedIn profiles based on queries
"""

import asyncio
import random
import logging
from typing import List, Dict, Optional
from urllib.parse import quote
from scraper.browser_controller import BrowserController
from scraper.human_behavior import HumanBehavior

logger = logging.getLogger(__name__)


class SearchAgent:
    """Agent for searching and collecting profile URLs"""
    
    def __init__(self, browser_controller: BrowserController):
        self.browser = browser_controller
        self.human_behavior = HumanBehavior()
    
    async def search_profiles(self, query: str, max_results: int = 100, 
                             location: Optional[str] = None) -> List[str]:
        """Search LinkedIn and collect profile URLs - with improved pagination"""
        profile_urls = []
        
        try:
            logger.info(f"🔍 Searching for profiles: '{query}'")
            print(f"[INFO] Searching for: '{query}'")
            
            # Build search URL
            search_url = f'https://www.linkedin.com/search/results/people/?keywords={quote(query)}'
            
            if location:
                search_url += f'&location={quote(location)}'
            
            # Navigate to search with extended timeout and better wait strategy
            max_search_retries = 5
            if not await self.browser.navigate(search_url, wait_until='domcontentloaded', timeout=60000, max_retries=max_search_retries):
                logger.error("[X] Failed to navigate to search page after multiple retries")
                return profile_urls
            
            # Add initial delay after search page loads
            await self.human_behavior.random_delay(2, 4)
            
            # Collect profiles from multiple pages with FULL pagination
            page = 1
            max_pages = 50  # LinkedIn typically shows up to 100 pages
            no_new_profiles_count = 0
            
            while len(profile_urls) < max_results and page <= max_pages:
                print(f"[INFO] Page {page}: Collecting profiles... (total so far: {len(profile_urls)})")
                logger.info(f"Collecting profiles from page {page}...")
                
                # Scroll to load more results on current page
                await self._scroll_search_results()
                await self.human_behavior.random_delay(1, 2)
                
                # Extract profile links
                prev_count = len(profile_urls)
                links = await self._extract_profile_links()
                
                for link in links:
                    if link not in profile_urls:
                        profile_urls.append(link)
                
                new_found = len(profile_urls) - prev_count
                
                if new_found == 0:
                    no_new_profiles_count += 1
                    if no_new_profiles_count >= 3:
                        print(f"[INFO] No more new profiles found. Total: {len(profile_urls)}")
                        break
                else:
                    no_new_profiles_count = 0
                
                logger.info(f"Page {page}: Found {new_found} new profiles (total: {len(profile_urls)})")
                
                # Check if we have enough
                if len(profile_urls) >= max_results:
                    print(f"[INFO] Reached requested limit of {max_results}")
                    break
                
                # Check for next page button
                has_next = await self._navigate_to_next_page()
                
                if not has_next:
                    logger.info("Reached end of search results")
                    print("[INFO] Reached end of search results")
                    break
                
                page += 1
                await self.human_behavior.random_delay(2, 4)
            
            print(f"[INFO] ===== Search completed: Found {len(profile_urls)} profiles =====")
            logger.info(f"Search completed: Found {len(profile_urls)} profiles")
            return profile_urls[:max_results]
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return profile_urls
    
    async def _scroll_search_results(self):
        """Scroll through search results to load all on current page"""
        try:
            # Multiple scroll passes to ensure all results load
            for _ in range(3):
                await self.browser.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await self.human_behavior.random_delay(0.5, 1)
            
            # Scroll back to top
            await self.browser.page.evaluate("window.scrollTo(0, 0)")
            await self.human_behavior.random_delay(0.3, 0.6)
        except:
            pass
    
    async def _extract_profile_links(self) -> List[str]:
        """Extract all profile links from current page"""
        try:
            links = await self.browser.page.evaluate("""
                () => {
                    const profileLinks = [];
                    const anchorElements = document.querySelectorAll('a');
                    
                    for (let anchor of anchorElements) {
                        const href = anchor.getAttribute('href');
                        if (href && href.includes('/in/')) {
                            // Clean URL (remove query parameters)
                            const cleanUrl = href.split('?')[0];
                            if (!cleanUrl.startsWith('http')) {
                                profileLinks.push('https://www.linkedin.com' + cleanUrl);
                            } else {
                                profileLinks.push(cleanUrl);
                            }
                        }
                    }
                    
                    // Remove duplicates
                    return [...new Set(profileLinks)];
                }
            """)
            
            logger.debug(f"Extracted {len(links)} profile links")
            return links
            
        except Exception as e:
            logger.error(f"Error extracting profile links: {e}")
            return []
    
    async def _navigate_to_next_page(self) -> bool:
        """Navigate to next page of search results"""
        try:
            # Look for next page button
            next_button = await self.browser.page.query_selector('button[aria-label*="Next"]')
            
            if next_button:
                await self.human_behavior.human_click(
                    self.browser.page,
                    'button[aria-label*="Next"]'
                )
                await asyncio.sleep(random.uniform(2, 4))
                return True
            
            return False
            
        except Exception as e:
            logger.debug(f"Error navigating to next page: {e}")
            return False
    
    async def collect_featured_profiles(self, query: str, num_profiles: int = 20) -> List[Dict]:
        """Collect profiles with additional metadata"""
        profile_urls = await self.search_profiles(query, max_results=num_profiles)
        
        profiles_with_meta = []
        for url in profile_urls:
            try:
                meta = {
                    'url': url,
                    'collected_at': self._get_timestamp(),
                    'source_query': query
                }
                profiles_with_meta.append(meta)
            except:
                continue
        
        return profiles_with_meta
    
    @staticmethod
    def _get_timestamp() -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().isoformat()
