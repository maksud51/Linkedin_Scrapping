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
        """Search LinkedIn and collect profile URLs - with improved retry logic"""
        profile_urls = []
        
        try:
            logger.info(f"🔍 Searching for profiles: '{query}'")
            
            # Build search URL
            search_url = f'https://www.linkedin.com/search/results/people/?keywords={quote(query)}'
            
            if location:
                search_url += f'&location={quote(location)}'
            
            # Navigate to search with extended timeout and better wait strategy
            # Use domcontentloaded for search pages (faster than networkidle)
            max_search_retries = 5  # Increase retries for search pages
            if not await self.browser.navigate(search_url, wait_until='domcontentloaded', timeout=60000, max_retries=max_search_retries):
                logger.error("[X] Failed to navigate to search page after multiple retries")
                return profile_urls
            
            # Add initial delay after search page loads
            await self.human_behavior.random_delay(2, 4)
            
            # Collect profiles from multiple pages
            page = 1
            max_pages = (max_results // 10) + 2  # Approximate pages needed
            
            while len(profile_urls) < max_results and page <= max_pages:
                logger.info(f"Collecting profiles from page {page}...")
                
                # Scroll to load more results
                await self.human_behavior.human_scroll(self.browser.page, scroll_pattern='natural')
                await self.human_behavior.random_delay(2, 4)
                
                # Extract profile links
                links = await self._extract_profile_links()
                
                for link in links:
                    if link not in profile_urls and len(profile_urls) < max_results:
                        profile_urls.append(link)
                
                logger.info(f"Collected {len(profile_urls)} profiles so far")
                
                # Check for next page button
                has_next = await self._navigate_to_next_page()
                
                if not has_next:
                    logger.info("Reached end of search results")
                    break
                
                page += 1
                await self.human_behavior.random_delay(3, 6)
            
            logger.info(f"Search completed: Found {len(profile_urls)} profiles")
            return profile_urls[:max_results]
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return profile_urls
    
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
