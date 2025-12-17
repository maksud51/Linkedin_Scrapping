"""
Connections Agent: Scrapes profiles from user's connections
Similar to SearchAgent but navigates via "Me" > "Connections" instead of search
"""

import asyncio
import random
import logging
from typing import List, Dict, Optional
from scraper.browser_controller import BrowserController
from scraper.human_behavior import HumanBehavior

logger = logging.getLogger(__name__)


class ConnectionsAgent:
    """Agent for collecting profile URLs from user's connections"""
    
    def __init__(self, browser_controller: BrowserController):
        self.browser = browser_controller
        self.human_behavior = HumanBehavior()
    
    async def navigate_to_my_profile(self) -> bool:
        """Navigate to user's own profile via 'Me' dropdown"""
        try:
            logger.info("[[LOCK]] Navigating to user's profile...")
            
            # Direct navigation to profile (easier than UI clicks)
            # Go to mynetwork first
            if not await self.browser.navigate('https://www.linkedin.com/mynetwork/', wait_until='domcontentloaded', timeout=60000, max_retries=2):
                logger.warning("[WARN] Failed to navigate to mynetwork")
                return False
            
            await self.human_behavior.random_delay(2, 3)
            
            logger.info("[[OK]] Successfully navigated to own area")
            return True
            
        except Exception as e:
            logger.error(f"[X] Error navigating to profile: {e}")
            return False
    
    async def navigate_to_connections(self) -> bool:
        """Navigate to connections section"""
        try:
            logger.info("[[LINK]] Navigating to connections...")
            
            # Direct navigation to connections
            if not await self.browser.navigate(
                'https://www.linkedin.com/mynetwork/invite-connect/connections/',
                wait_until='domcontentloaded',
                timeout=60000,
                max_retries=2
            ):
                logger.warning("[WARN] Could not navigate to connections")
                return False
            
            # Wait for connections page to load
            await self.human_behavior.random_delay(2, 4)
            
            logger.info("[[OK]] Successfully navigated to connections")
            return True
            
        except Exception as e:
            logger.error(f"[X] Error navigating to connections: {e}")
            return False
    
    async def collect_connection_profiles(self, max_results: int = 100) -> List[str]:
        """Collect profile URLs from connections (similar to search)"""
        profile_urls = []
        
        try:
            # Navigate to own profile first
            if not await self.navigate_to_my_profile():
                logger.error("[X] Failed to navigate to own profile")
                return profile_urls
            
            # Navigate to connections section
            if not await self.navigate_to_connections():
                logger.error("[X] Failed to navigate to connections")
                return profile_urls
            
            logger.info(f"[[LIST]] Collecting connection profiles (max: {max_results})...")
            
            page = 1
            max_pages = (max_results // 10) + 2
            
            while len(profile_urls) < max_results and page <= max_pages:
                logger.info(f"Collecting profiles from connections page {page}...")
                
                # Scroll to load more profiles
                await self.human_behavior.human_scroll(
                    self.browser.page,
                    scroll_pattern='natural'
                )
                await self.human_behavior.random_delay(2, 4)
                
                # Extract profile links from connections list
                links = await self._extract_connection_links()
                
                for link in links:
                    if link not in profile_urls and len(profile_urls) < max_results:
                        profile_urls.append(link)
                
                logger.info(f"Collected {len(profile_urls)} profiles so far")
                
                # Try to navigate to next page
                has_next = await self._navigate_to_next_page()
                
                if not has_next:
                    logger.info("Reached end of connections list")
                    break
                
                page += 1
                await self.human_behavior.random_delay(3, 6)
            
            logger.info(f"[[OK]] Collection complete: Found {len(profile_urls)} connection profiles")
            return profile_urls[:max_results]
            
        except Exception as e:
            logger.error(f"[X] Error collecting connection profiles: {e}")
            return profile_urls
    
    async def _extract_connection_links(self) -> List[str]:
        """Extract all connection profile links from current page"""
        try:
            links = await self.browser.page.evaluate("""
                () => {
                    const profileLinks = [];
                    const anchorElements = document.querySelectorAll('a');
                    
                    for (let anchor of anchorElements) {
                        const href = anchor.getAttribute('href');
                        // Match LinkedIn profile URLs in connections list
                        if (href && href.includes('/in/') && !href.includes('/overlay/')) {
                            // Clean URL (remove query parameters)
                            const cleanUrl = href.split('?')[0];
                            const fullUrl = cleanUrl.startsWith('http') 
                                ? cleanUrl 
                                : 'https://www.linkedin.com' + cleanUrl;
                            
                            profileLinks.push(fullUrl);
                        }
                    }
                    
                    // Remove duplicates and filter out LinkedIn internal links
                    const uniqueLinks = [...new Set(profileLinks)].filter(url => {
                        return !url.includes('/search/results/') && 
                               !url.includes('/notifications/') &&
                               !url.includes('/messaging/');
                    });
                    
                    return uniqueLinks;
                }
            """)
            
            logger.debug(f"Extracted {len(links)} connection profile links")
            return links
            
        except Exception as e:
            logger.error(f"Error extracting connection links: {e}")
            return []
    
    async def _navigate_to_next_page(self) -> bool:
        """Navigate to next page of connections"""
        try:
            # Look for next page button or pagination
            next_button_selector = 'button[aria-label*="Next"]'
            
            # Check if button exists and is clickable
            next_button = await self.browser.page.query_selector(next_button_selector)
            
            if not next_button:
                return False
            
            # Check if button is disabled
            is_disabled = await next_button.evaluate('el => el.disabled')
            
            if is_disabled:
                return False
            
            # Click next button
            success = await self.human_behavior.human_click(
                self.browser.page,
                selector=next_button_selector
            )
            
            if success:
                await asyncio.sleep(random.uniform(2, 4))
            
            return success
            
        except Exception as e:
            logger.debug(f"Error navigating to next connections page: {e}")
            return False
    
    async def scrape_connection_profiles(self, scrape_agent, db_manager, max_profiles: int = 100) -> Dict:
        """
        Complete workflow: Collect connections and scrape them
        Similar to search + scrape workflow
        """
        try:
            logger.info(f"[[CYCLE]] Starting connections scraping workflow (max: {max_profiles})...")
            
            # Step 1: Collect connection profile URLs
            profile_urls = await self.collect_connection_profiles(max_results=max_profiles)
            
            if not profile_urls:
                logger.warning("[WARN] No connection profiles collected")
                return {"success": False, "total": 0, "scraped": 0, "skipped": 0}
            
            logger.info(f"[[STAT]] Collected {len(profile_urls)} connection profiles")
            
            # Step 2: Add profiles to database queue first
            logger.info(f"[[NOTE]] Adding {len(profile_urls)} profiles to database queue...")
            db_manager.add_profiles(profile_urls)
            
            # Step 3: Scrape each profile (using provided scrape_agent)
            scraped_count = 0
            skipped_count = 0
            failed_count = 0
            
            for idx, url in enumerate(profile_urls, 1):
                try:
                    logger.info(f"[{idx}/{len(profile_urls)}] Processing connection: {url}")
                    
                    # Check if profile already exists in database
                    if db_manager.is_profile_scraped(url):
                        logger.info(f"   [SKIP] Profile already scraped")
                        skipped_count += 1
                        continue
                    
                    # Scrape profile
                    profile_data = await scrape_agent.scrape_profile(url)
                    
                    if profile_data:
                        # Save to database
                        completeness = 0.8  # Connections usually have less detail
                        db_manager.save_profile_data(url, profile_data, completeness)
                        scraped_count += 1
                        logger.info(f"   [[OK]] Saved: {profile_data.get('name', 'Unknown')}")
                    else:
                        failed_count += 1
                        logger.warning(f"   [X] Failed to scrape")
                    
                    # Add delay between scrapes (anti-detection)
                    await self.human_behavior.random_delay(random.uniform(5, 15))
                    
                except Exception as e:
                    logger.error(f"   [X] Error processing {url}: {e}")
                    failed_count += 1
                    continue
            
            result = {
                "success": True,
                "total": len(profile_urls),
                "scraped": scraped_count,
                "skipped": skipped_count,
                "failed": failed_count,
                "urls": profile_urls
            }
            
            logger.info(f"[[OK]] Connections scraping complete: {scraped_count} scraped, {skipped_count} skipped, {failed_count} failed")
            return result
            
        except Exception as e:
            logger.error(f"[X] Error in connections scraping workflow: {e}")
            return {"success": False, "total": 0, "scraped": 0, "skipped": 0}
