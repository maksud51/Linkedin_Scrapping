"""
Scrape Agent: Extracts profile data
"""

import asyncio
import logging
import random
from typing import Optional, Dict, List
from scraper.browser_controller import BrowserController
from scraper.data_extractor import DataExtractor
from scraper.human_behavior import HumanBehavior

logger = logging.getLogger(__name__)


class ScrapeAgent:
    """Agent for scraping profile data"""
    
    def __init__(self, browser_controller: BrowserController, data_extractor: DataExtractor):
        self.browser = browser_controller
        self.data_extractor = data_extractor
        self.human_behavior = HumanBehavior()
        # Allow data_extractor to call back to us for contact info extraction
        self.data_extractor.scrape_agent = self
    
    async def scrape_profile(self, profile_url: str) -> Optional[Dict]:
        """Scrape single profile with comprehensive extraction"""
        try:
            logger.info(f"[SCRAPE] Scraping profile: {profile_url}")
            
            # Navigate to profile with extended timeout and retry
            if not await self.browser.navigate(profile_url, wait_until='domcontentloaded', timeout=60000, max_retries=3):
                logger.warning(f"[WARN] Failed to navigate to {profile_url}")
                return None
            
            # Wait for page to stabilize after navigation
            await self.human_behavior.random_delay(1, 2)
            
            # Close any modal dialogs (e.g., app upsell prompts)
            try:
                # Try multiple selector strategies to close modals
                modal_selectors = [
                    'button[aria-label="Close"]',
                    'button[aria-label="Dismiss"]',
                    '[role="dialog"] button:first-child',
                    '.cta-modal button',
                ]
                for selector in modal_selectors:
                    try:
                        close_btn = await self.browser.page.query_selector(selector)
                        if close_btn:
                            await close_btn.click()
                            await self.human_behavior.random_delay(0.5, 1)
                            break
                    except:
                        pass
            except:
                pass
            
            # Check for access issues
            if await self._check_profile_access_issues():
                logger.warning(f"Profile access restricted: {profile_url}")
                return None
            
            # Human-like behavior
            await self.human_behavior.human_scroll(self.browser.page, scroll_pattern='natural')
            await self.human_behavior.random_mouse_movement(self.browser.page)
            await self.human_behavior.random_delay(2, 4)
            
            # IMPORTANT: Extract profile data (includes contact info if available)
            # Contact info is extracted during extract_complete_profile via data_extractor
            profile_data = await self.data_extractor.extract_complete_profile(
                self.browser.page,
                profile_url
            )
            
            if not profile_data:
                logger.warning(f"No data extracted from: {profile_url}")
                return None
            
            # Expand sections
            await self._expand_all_sections()
            
            if profile_data:
                logger.info(f"Successfully scraped: {profile_data.get('name', 'Unknown')}")
                logger.debug(f"Profile sections: {list(profile_data.keys())}")
                if 'contact_info' in profile_data:
                    logger.info(f"Contact info extracted: {list(profile_data['contact_info'].keys())}")
                return profile_data
            else:
                logger.warning(f"No data extracted from: {profile_url}")
                return None
                
        except Exception as e:
            logger.error(f"Error scraping {profile_url}: {e}")
            return None
    
    async def _check_profile_access_issues(self) -> bool:
        """Check if profile has access restrictions - strict check"""
        try:
            page_content = await self.browser.get_page_content()
            page_text = page_content.lower()
            
            # More specific access issue patterns
            access_issues = [
                "this profile is not available",
                "you cannot view this profile",
                "profile is not public",
                "profile private",
                "404 error",
                "not found",
            ]
            
            return any(issue.lower() in page_text for issue in access_issues)
            
        except Exception as e:
            logger.debug(f"Error checking access: {e}")
            return False
    
    async def _expand_all_sections(self):
        """Expand all collapsible sections on profile - comprehensive approach"""
        try:
            # Close any modal dialogs that might interfere
            try:
                close_buttons = await self.browser.page.query_selector_all(
                    'button[aria-label="Close"], button[aria-label="Dismiss"], [role="dialog"] button:first-child'
                )
                for btn in close_buttons[:3]:  # Close first 3 modals
                    try:
                        await btn.click()
                        await self.human_behavior.random_delay(0.5, 1)
                    except:
                        pass
            except:
                pass
            
            # Multiple passes to catch dynamically generated buttons
            expand_attempts = 0
            max_attempts = 3
            
            while expand_attempts < max_attempts:
                expand_attempts += 1
                
                # Find all expandable elements with multiple selectors
                buttons = await self.browser.page.query_selector_all(
                    'button[aria-expanded="false"], '
                    'button:has-text("Show more"), '
                    'button:has-text("See more"), '
                    'button:has-text("See all"), '
                    '.inline-show-more-text__button, '
                    '[class*="show-more"] button'
                )
                
                logger.debug(f"Expansion attempt {expand_attempts}: Found {len(buttons)} expandable sections")
                
                if len(buttons) == 0:
                    break
                
                for i, button in enumerate(buttons[:20]):  # Increase limit
                    try:
                        await button.scroll_into_view_if_needed()
                        await self.human_behavior.random_delay(0.2, 0.6)
                        await button.click()
                        await self.human_behavior.random_delay(0.4, 1.2)
                        
                        # Occasional longer pause
                        if i % 5 == 0:
                            await self.human_behavior.random_delay(1, 2)
                            
                    except Exception as e:
                        logger.debug(f"Could not expand section {i}: {e}")
                        continue
                
                # Small delay between passes
                await self.human_behavior.random_delay(1, 2)
                    
        except Exception as e:
            logger.debug(f"Error expanding sections: {e}")
    
    async def _parse_overlay_html(self, html: str) -> Optional[str]:
        """Extract contact information text from overlay HTML"""
        try:
            import re
            
            # Remove script and style tags
            html_clean = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
            html_clean = re.sub(r'<style[^>]*>.*?</style>', '', html_clean, flags=re.DOTALL)
            
            # Get text content
            import html as html_module
            text = re.sub(r'<[^>]+>', ' ', html_clean)  # Remove HTML tags
            text = html_module.unescape(text)  # Decode HTML entities
            
            # Split into lines and clean
            lines = text.split('\n')
            lines = [line.strip() for line in lines if line.strip() and len(line.strip()) > 1]
            
            # Filter out noise - keep only lines with meaningful content
            # Contact info sections usually have keywords
            keywords = ['linkedin', 'website', 'email', 'phone', 'twitter', 'github', 'facebook', 'instagram', 'contact', 
                       'birthday', 'born', 'whatsapp', 'telegram', 'skype', 'youtube', 'https', 'http', '@', '.com', '.org', '.net']
            
            contact_lines = []
            for line in lines:
                # Check if line contains contact-related keywords or looks like a domain/email
                if any(kw in line.lower() for kw in keywords):
                    contact_lines.append(line)
                elif re.search(r'[\w\-]+\.[\w]{2,}', line):  # Looks like domain/URL
                    contact_lines.append(line)
                elif re.search(r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}', line):  # Phone pattern
                    contact_lines.append(line)
                elif re.search(r'[A-Za-z]+\s+\d{1,2}', line):  # Date pattern (like April 8)
                    contact_lines.append(line)
            
            if contact_lines:
                result = '\n'.join(contact_lines)
                return result if len(result) > 20 else None
            
            return None
        except Exception as e:
            logger.debug(f"Error parsing overlay HTML: {e}")
            return None
    
    async def _extract_contact_info(self) -> Optional[Dict]:
        """Extract contact info by navigating to contact-info overlay"""
        try:
            logger.info("Attempting to extract contact info...")
            
            # Get current profile URL
            current_url = self.browser.page.url
            if not current_url or '/in/' not in current_url:
                logger.warning("Not on a profile page")
                return None
            
            # Quick attempt: Try direct overlay navigation first (Method 2 priority)
            logger.debug("Quick Method: Trying direct overlay navigation...")
            try:
                if '/in/' in current_url:
                    overlay_url = current_url.rstrip('/') + '/overlay/contact-info/'
                    logger.debug(f"Navigating to overlay: {overlay_url}")
                    
                    response = await self.browser.navigate(
                        overlay_url,
                        wait_until='domcontentloaded',
                        timeout=8000,
                        max_retries=1
                    )
                    
                    if response:
                        await self.human_behavior.random_delay(0.5, 1)
                        
                        # Extract STRUCTURED contact info from overlay
                        page_html = await self.browser.page.content()

                        # --- Extract full website URLs from href attributes (e.g. sites.google.com/diu.edu.bd/...) ---
                        href_websites: list[str] = []
                        try:
                            import re  # Local import to keep changes minimal

                            hrefs = re.findall(r'href="([^"]+)"', page_html)
                            for href in hrefs:
                                href_clean = href.strip()
                                href_lower = href_clean.lower()

                                # Skip non-URL schemes
                                if href_lower.startswith(("mailto:", "tel:", "javascript:")):
                                    continue

                                # Skip LinkedIn/GitHub (handled separately)
                                if "linkedin.com" in href_lower or "github.com" in href_lower:
                                    continue

                                # Skip LICDN static/assets and obvious media files
                                if "static.licdn.com" in href_lower or "media.licdn.com" in href_lower:
                                    continue
                                if href_lower.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")):
                                    continue

                                # Skip obvious non-external URLs
                                if not href_clean or href_clean.startswith("#") or href_clean.startswith("/"):
                                    continue

                                href_websites.append(href_clean)

                            # Deduplicate while preserving order
                            deduped: list[str] = []
                            for url in href_websites:
                                if url not in deduped:
                                    deduped.append(url)
                            href_websites = deduped
                        except Exception as e:
                            logger.debug(f"Error extracting href websites from contact overlay: {e}")
                            href_websites = []
                        # --- END href extraction ---

                        contact_text = await self._parse_overlay_html(page_html)
                        
                        if contact_text and len(contact_text) > 50:
                            logger.debug(f"Got contact info from overlay: {len(contact_text)} chars")
                            
                            # Parse and return
                            contact_info = self.data_extractor.parse_contact_info(contact_text)

                            # Merge in any websites we found directly from href attributes
                            # so full URLs (with paths) are preserved.
                            if href_websites:
                                existing_websites = contact_info.get("websites")
                                merged: list[str] = []

                                if isinstance(existing_websites, list) and existing_websites:
                                    merged.extend(existing_websites)

                                merged.extend(href_websites)

                                # Final de-duplication while preserving order
                                final_websites: list[str] = []
                                for url in merged:
                                    if url not in final_websites:
                                        final_websites.append(url)

                                if final_websites:
                                    contact_info["websites"] = final_websites
                            
                            # Navigate back to original profile
                            try:
                                await self.browser.navigate(current_url, wait_until='domcontentloaded', timeout=8000)
                            except:
                                pass
                            
                            if contact_info:
                                return contact_info
            except Exception as e:
                logger.debug(f"Quick method failed: {e}")
            
            logger.info("Contact info not extracted (may require premium or not available)")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting contact info: {e}", exc_info=True)
            return None
    
    async def scrape_multiple_profiles(self, profile_urls: List[str], 
                                     delay_range: tuple = (15, 30)) -> Dict[str, Dict]:
        """Scrape multiple profiles with intelligent delays"""
        results = {
            'total': len(profile_urls),
            'successful': 0,
            'failed': 0,
            'profiles': []
        }
        
        for i, profile_url in enumerate(profile_urls, 1):
            try:
                logger.info(f"Progress: {i}/{len(profile_urls)} ({i/len(profile_urls)*100:.1f}%)")
                
                # Intelligent rate limiting
                if i > 1:
                    await self._adaptive_delay(i, len(profile_urls), delay_range)
                
                # Scrape profile
                profile_data = await self.scrape_profile(profile_url)
                
                if profile_data:
                    results['successful'] += 1
                    results['profiles'].append(profile_data)
                else:
                    results['failed'] += 1
                
            except Exception as e:
                logger.error(f"Error in bulk scrape: {e}")
                results['failed'] += 1
                continue
        
        logger.info(f"Scraping completed: {results['successful']}/{results['total']} successful")
        return results
    
    async def _adaptive_delay(self, current: int, total: int, base_range: tuple):
        """Intelligent delay that adapts based on progress"""
        base_min, base_max = base_range
        
        # Increase delay as progress increases (LinkedIn detects patterns)
        progress_factor = current / total
        
        if progress_factor > 0.7:  # Last 30%
            delay_min = base_min * 1.5
            delay_max = base_max * 1.5
        elif progress_factor > 0.9:  # Last 10%
            delay_min = base_min * 2.0
            delay_max = base_max * 2.0
        else:
            delay_min = base_min
            delay_max = base_max
        
        delay = random.uniform(delay_min, delay_max)
        logger.info(f"⏳ Waiting {delay:.1f} seconds (anti-detection)...")
        await asyncio.sleep(delay)
