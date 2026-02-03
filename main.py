"""
Advanced LinkedIn Bulk Profile Scraper
Main entry point with CLI interface and complete workflow

Features:
- Multi-agent architecture (Search, Scrape, Validate)
- Text-based data extraction (resistant to HTML changes)
- Advanced anti-detection (fingerprinting, stealth, CAPTCHA handling)
- Resume capability (SQLite with progress tracking)
- Adaptive rate limiting and human-like behavior
- Export to JSON/CSV/Excel with statistics
"""

import asyncio
import sys
import os
from pathlib import Path
from datetime import datetime
import logging

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import components
from utils.logger import setup_logging, get_logger
from utils.config import Config
from utils.helpers import print_banner, print_config_info
from utils.exporter import DataExporter
from scraper.browser_controller import BrowserController
from scraper.data_extractor import DataExtractor
from agents.search_agent import SearchAgent
from agents.scrape_agent import ScrapeAgent
from agents.validation_agent import ValidationAgent
from agents.connections_agent import ConnectionsAgent
from database.db_manager import DatabaseManager

logger = logging.getLogger(__name__)


class LinkedInScraperApp:
    """Main application with complete workflow"""
    
    def __init__(self):
        """Initialize application"""
        self.config = Config()
        self.db = DatabaseManager(self.config.database['path'])
        self.browser_controller: BrowserController = None
        self.data_extractor: DataExtractor = None
        self.search_agent: SearchAgent = None
        self.scrape_agent: ScrapeAgent = None
        self.validation_agent: ValidationAgent = None
        self.connections_agent: ConnectionsAgent = None
        self.exporter: DataExporter = None
        self.start_time = None
    
    async def initialize(self) -> bool:
        """Initialize all components"""
        try:
            logger.info("Initializing LinkedIn Scraper App...")
            
            # Browser controller
            self.browser_controller = BrowserController(
                headless=self.config.HEADLESS,
                use_proxy=self.config.browser.get('proxy_server') if self.config.browser.get('use_proxy') else None,
                use_stealth=self.config.scraping['use_stealth']
            )
            
            if not await self.browser_controller.initialize():
                logger.error("[X] Browser initialization failed")
                return False
            
            # Components
            self.data_extractor = DataExtractor()
            self.search_agent = SearchAgent(self.browser_controller)
            self.scrape_agent = ScrapeAgent(self.browser_controller, self.data_extractor)
            self.validation_agent = ValidationAgent()
            self.connections_agent = ConnectionsAgent(self.browser_controller)
            self.exporter = DataExporter(self.config.export['export_path'])
            
            logger.info("[OK] All components initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"[X] Initialization failed: {e}")
            return False
    
    async def login(self) -> bool:
        """Login to LinkedIn"""
        try:
            logger.info("[LOCK] Attempting LinkedIn login...")
            
            if not self.config.LINKEDIN_EMAIL or not self.config.LINKEDIN_PASSWORD:
                logger.error("[X] LinkedIn credentials not configured")
                logger.error("   Set LINKEDIN_EMAIL and LINKEDIN_PASSWORD in .env file")
                return False
            
            # Navigate to login
            if not await self.browser_controller.navigate('https://www.linkedin.com/login', timeout=60000, max_retries=3):
                return False
            
            # Wait for page to load
            await asyncio.sleep(2)
            
            # Type email
            from scraper.human_behavior import HumanBehavior
            human_behavior = HumanBehavior()
            
            await human_behavior.human_type(
                self.browser_controller.page,
                '#username',
                self.config.LINKEDIN_EMAIL
            )
            await human_behavior.random_delay(1, 2)
            
            # Type password
            await human_behavior.human_type(
                self.browser_controller.page,
                '#password',
                self.config.LINKEDIN_PASSWORD
            )
            await human_behavior.random_delay(1, 2)
            
            # Click login
            await self.browser_controller.page.click('button[type="submit"]')
            
            # Wait for navigation
            try:
                await self.browser_controller.page.wait_for_url('**/feed/**', timeout=20000)
                logger.info("[OK] Login successful")
                return True
            except:
                logger.warning("[WARN] Login may have timed out or required additional verification")
                # Check if we're at feed or checkpoint
                current_url = self.browser_controller.page.url
                if 'feed' in current_url:
                    logger.info("[OK] Login successful (feed detected)")
                    return True
                elif 'checkpoint' in current_url:
                    logger.warning("[LOCK] Additional security verification required")
                    print("\n" + "="*60)
                    print("Please complete the security verification in the browser")
                    print("The script will wait for 3 minutes...")
                    print("="*60)
                    try:
                        await self.browser_controller.page.wait_for_url('**/feed/**', timeout=180000)
                        logger.info("[OK] Verification completed")
                        return True
                    except:
                        logger.error("[X] Verification timeout")
                        return False
                return False
                
        except Exception as e:
            logger.error(f"[X] Login error: {e}")
            return False
    
    async def workflow_search_and_scrape(self, search_queries: list, max_profiles_per_query: int = 50):
        """Complete workflow: Search → Scrape → Validate → Export with full pagination"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info("WORKFLOW: SEARCH AND SCRAPE")
            logger.info(f"{'='*60}\n")
            
            session_id = self.db.create_search_session(f"batch_{len(search_queries)}_queries")
            
            all_profiles = []
            
            for query_idx, query in enumerate(search_queries, 1):
                logger.info(f"\nQuery {query_idx}/{len(search_queries)}: '{query}'")
                logger.info("-" * 60)
                
                # Search profiles with FULL pagination
                print(f"[INFO] Searching for profiles with query: '{query}'...")
                logger.info("Searching for profiles with full pagination...")
                
                # Search agent already handles pagination, get ALL results
                all_found_urls = await self.search_agent.search_profiles(
                    query,
                    max_results=500  # Get more results initially
                )
                
                if not all_found_urls:
                    logger.warning(f"No profiles found for query: {query}")
                    continue
                
                print(f"[INFO] ===== TOTAL PROFILES FOUND: {len(all_found_urls)} =====")
                logger.info(f"[INFO] Total profiles found: {len(all_found_urls)}")
                
                # Filter out already scraped profiles
                print("[INFO] Checking database for already scraped profiles...")
                unscraped_urls = self.db.get_unscraped_profiles(all_found_urls)
                
                already_scraped = len(all_found_urls) - len(unscraped_urls)
                if already_scraped > 0:
                    print(f"[INFO] Skipping {already_scraped} already scraped profiles")
                    logger.info(f"[INFO] Skipping {already_scraped} already scraped profiles")
                
                print(f"[INFO] {len(unscraped_urls)} profiles available for scraping")
                
                if not unscraped_urls:
                    print(f"[INFO] All profiles for '{query}' already scraped!")
                    continue
                
                # Limit to requested number from unscraped
                profile_urls = unscraped_urls[:max_profiles_per_query]
                print(f"[INFO] Will scrape {len(profile_urls)} profiles (requested: {max_profiles_per_query})")
                
                # Create history entry for this query
                history_id = self.db.create_scraping_history(
                    session_type='search',
                    query_or_source=query,
                    total_found=len(all_found_urls),
                    total_requested=max_profiles_per_query
                )
                
                # Add to database
                added = self.db.add_profiles(profile_urls, session_id)
                self.db.add_profiles_to_queue(history_id, profile_urls)
                logger.info(f"[OK] Added {added} profiles to queue")
                
                # Scrape profiles
                logger.info(f"Scraping {len(profile_urls)} profiles...")
                scrape_results = await self.scrape_agent.scrape_multiple_profiles(
                    profile_urls,
                    delay_range=self.config.scraping['delay_between_profiles']
                )
                
                # Mark scraped profiles in database
                scraped_urls = set()
                for profile_data in scrape_results['profiles']:
                    if profile_data:
                        profile_url = profile_data.get('profile_url', '')
                        completeness = profile_data.get('completeness', 0)
                        self.db.save_profile_data(profile_url, profile_data, completeness)
                        self.db.update_queue_status(history_id, profile_url, 'completed')
                        scraped_urls.add(profile_url)

                # Mark any profile URLs that were not scraped as failed
                for url in profile_urls:
                    if url not in scraped_urls:
                        self.db.mark_profile_failed(url, "Navigation/Access failed or blocked")
                        self.db.update_queue_status(history_id, url, 'failed')
                
                # Update history stats
                self.db.update_history_stats(history_id)
                
                # Validate scraped data
                logger.info("Validating scraped data...")
                validation_results = self.validation_agent.batch_validate(scrape_results['profiles'])
                
                logger.info(f"[OK] Validation: {validation_results['valid']}/{validation_results['total']} valid")
                logger.info(f"[STAT] Avg Completeness: {validation_results['avg_completeness']}%")
                logger.info(f"[STAT] Avg Score: {validation_results['avg_score']}/100")
                
                # Store validated profiles
                all_profiles.extend(scrape_results['profiles'])
                
                # Show progress
                stats = self.db.get_scraping_stats()
                logger.info(f"\nOverall Progress:")
                logger.info(f"   Total: {stats['total']}")
                logger.info(f"   Completed: {stats['completed']}")
                logger.info(f"   Failed: {stats['failed']}")
                logger.info(f"   Pending: {stats['pending']}")
                logger.info(f"   Success Rate: {stats['success_rate']}")
            
            # Export data
            logger.info(f"\n{'='*60}")
            logger.info("EXPORTING DATA")
            logger.info(f"{'='*60}\n")
            
            export_results = self.exporter.export_all_formats(all_profiles)
            
            for format_name, success in export_results.items():
                if success:
                    logger.info(f"[OK] Exported to {format_name.upper()}")
                elif success is None:
                    logger.warning(f"[WARN] {format_name.upper()} export skipped (library not installed)")
                else:
                    logger.error(f"[X] Failed to export to {format_name.upper()}")
            
            # Final statistics
            final_stats = self.db.get_scraping_stats()
            logger.info(f"\n{'='*60}")
            logger.info("FINAL STATISTICS")
            logger.info(f"{'='*60}")
            logger.info(f"Total Profiles: {final_stats['total']}")
            logger.info(f"Completed: {final_stats['completed']}")
            logger.info(f"Failed: {final_stats['failed']}")
            logger.info(f"Pending: {final_stats['pending']}")
            logger.info(f"Success Rate: {final_stats['success_rate']}")
            logger.info(f"Avg Completeness: {final_stats['avg_completeness']}")
            logger.info(f"Database Size: {self.db.get_db_size()}")
            logger.info(f"Export Path: {self.exporter.get_export_path()}")
            logger.info(f"{'='*60}\n")
            
        except Exception as e:
            logger.error(f"[X] Workflow error: {e}")
    
    async def workflow_resume(self, limit: int = 100):
        """Resume scraping from last checkpoint with history tracking"""
        try:
            logger.info("RESUMING SCRAPING FROM CHECKPOINT")
            logger.info(f"{'='*60}\n")
            
            # Show recent history first
            history = self.db.get_scraping_history(limit=10)
            if history:
                print("\n[INFO] Recent Scraping History:")
                print("-" * 80)
                for h in history:
                    status_icon = "✓" if h['status'] == 'completed' else "⏳" if h['status'] == 'active' else "✗"
                    print(f"  {status_icon} #{h['id']} [{h['type']}] {h['query'][:30]}... | "
                          f"Found: {h['total_found']}, Scraped: {h['scraped']}/{h['requested']}, "
                          f"Pending: {h['pending']}")
                print("-" * 80)
            
            # Get pending profiles - first from queue, then from profiles table
            pending = self.db.get_pending_from_history(limit=limit)
            
            if not pending:
                # Fallback to old method
                pending = self.db.get_pending_profiles(limit)
            
            if not pending:
                logger.info("[OK] No pending profiles to resume")
                print("[INFO] No pending profiles to resume!")
                return
            
            print(f"\n[INFO] Found {len(pending)} pending profiles to resume")
            logger.info(f"Found {len(pending)} pending profiles")
            
            # Create history entry for resume
            history_id = self.db.create_scraping_history(
                session_type='resume',
                query_or_source=f'Resume {len(pending)} profiles',
                total_found=len(pending),
                total_requested=len(pending)
            )
            self.db.add_profiles_to_queue(history_id, pending)
            
            # Scrape
            results = await self.scrape_agent.scrape_multiple_profiles(
                pending,
                delay_range=self.config.scraping['delay_between_profiles']
            )
            
            # Save scraped data to database
            scraped_urls = set()
            for profile_data in results['profiles']:
                if profile_data:
                    profile_url = profile_data.get('profile_url', '')
                    completeness = profile_data.get('completeness', 0)
                    self.db.save_profile_data(profile_url, profile_data, completeness)
                    self.db.update_queue_status(history_id, profile_url, 'completed')
                    scraped_urls.add(profile_url)
            
            # Mark failed
            for url in pending:
                if url not in scraped_urls:
                    self.db.mark_profile_failed(url, "Resume failed")
                    self.db.update_queue_status(history_id, url, 'failed')
            
            # Update history
            self.db.update_history_stats(history_id)
            
            # Validate
            validation_results = self.validation_agent.batch_validate(results['profiles'])
            logger.info(f"[OK] Validation: {validation_results['valid']}/{validation_results['total']} valid")
            
            # Export ALL data (existing + new)
            all_profiles = self.db.get_all_scraped_data()
            if all_profiles:
                self.exporter.export_all_formats(all_profiles)
                print(f"[OK] Exported {len(all_profiles)} total profiles")
            
            # Stats
            final_stats = self.db.get_scraping_stats()
            logger.info(f"\nFinal Statistics: {final_stats}")
            print(f"\n[SUCCESS] Resume completed! Scraped: {len(scraped_urls)}/{len(pending)}")
            
        except Exception as e:
            logger.error(f"[X] Resume error: {e}")
            print(f"[ERROR] Resume error: {e}")
    
    async def workflow_retry_failed(self, limit: int = 50):
        """Retry scraping failed profiles"""
        try:
            logger.info("RETRYING FAILED PROFILES")
            logger.info(f"{'='*60}\n")
            
            # Get failed profiles
            failed_profiles = self.db.get_failed_profiles(limit)
            
            if not failed_profiles:
                print("[INFO] No failed profiles to retry!")
                return
            
            print(f"\n[INFO] Found {len(failed_profiles)} failed profiles to retry")
            
            # Reset them to pending
            self.db.reset_failed_to_pending(failed_profiles)
            
            # Create history entry
            history_id = self.db.create_scraping_history(
                session_type='retry',
                query_or_source=f'Retry {len(failed_profiles)} failed profiles',
                total_found=len(failed_profiles),
                total_requested=len(failed_profiles)
            )
            self.db.add_profiles_to_queue(history_id, failed_profiles)
            
            # Scrape
            results = await self.scrape_agent.scrape_multiple_profiles(
                failed_profiles,
                delay_range=self.config.scraping['delay_between_profiles']
            )
            
            # Save scraped data
            scraped_urls = set()
            for profile_data in results['profiles']:
                if profile_data:
                    profile_url = profile_data.get('profile_url', '')
                    completeness = profile_data.get('completeness', 0)
                    self.db.save_profile_data(profile_url, profile_data, completeness)
                    self.db.update_queue_status(history_id, profile_url, 'completed')
                    scraped_urls.add(profile_url)
            
            # Mark still failed
            for url in failed_profiles:
                if url not in scraped_urls:
                    self.db.mark_profile_failed(url, "Retry failed again")
                    self.db.update_queue_status(history_id, url, 'failed')
            
            # Update history
            self.db.update_history_stats(history_id)
            
            # Export all data
            all_profiles = self.db.get_all_scraped_data()
            if all_profiles:
                self.exporter.export_all_formats(all_profiles)
                print(f"[OK] Exported {len(all_profiles)} total profiles")
            
            print(f"\n[SUCCESS] Retry completed! Fixed: {len(scraped_urls)}/{len(failed_profiles)}")
            
        except Exception as e:
            logger.error(f"[X] Retry error: {e}")
            print(f"[ERROR] Retry error: {e}")
    
    async def workflow_scrape_connections(self, max_profiles: int = 50):
        """Scrape profiles from user's connections with full pagination"""
        print(f"\n[DEBUG] workflow_scrape_connections called with max_profiles={max_profiles}")
        try:
            logger.info(f"\n{'='*60}")
            logger.info("SCRAPING CONNECTIONS PROFILES")
            logger.info(f"{'='*60}\n")
            
            # Navigate to connections
            print("[INFO] Navigating to LinkedIn connections page...")
            logger.info("[INFO] Navigating to LinkedIn connections page...")
            
            connections_url = 'https://www.linkedin.com/mynetwork/invite-connect/connections/'
            if not await self.browser_controller.navigate(connections_url, wait_until='domcontentloaded', timeout=30000):
                logger.error("[X] Failed to navigate to connections page")
                print("[ERROR] Failed to navigate to connections page")
                return
            
            print("[INFO] Waiting for page to load...")
            await asyncio.sleep(4)
            
            # ===== SCROLL TO LOAD ALL CONNECTIONS =====
            print("[INFO] Scrolling to load all connections (this may take a while)...")
            logger.info("[INFO] Scrolling to load ALL connections with pagination...")
            
            from scraper.human_behavior import HumanBehavior
            human_behavior = HumanBehavior()
            
            all_profile_urls = set()
            scroll_attempts = 0
            max_scroll_attempts = 100  # Increase for more connections
            no_new_profiles_count = 0
            last_height = 0
            
            while scroll_attempts < max_scroll_attempts:
                # Extract current profile URLs using multiple selectors
                current_urls = await self.browser_controller.page.evaluate("""
                    () => {
                        const urls = [];
                        // Try multiple selectors for connection cards
                        const selectors = [
                            'a[href*="/in/"]',
                            '.mn-connection-card a[href*="/in/"]',
                            '.mn-connection-card__link',
                            '[data-control-name="connection_profile"] a',
                            '.scaffold-finite-scroll__content a[href*="/in/"]'
                        ];
                        
                        for (const selector of selectors) {
                            const links = document.querySelectorAll(selector);
                            for (let link of links) {
                                const href = link.getAttribute('href');
                                if (href && href.includes('/in/') && !href.includes('/overlay/')) {
                                    const cleanUrl = href.split('?')[0];
                                    const fullUrl = cleanUrl.startsWith('http') ? cleanUrl : 'https://www.linkedin.com' + cleanUrl;
                                    if (!urls.includes(fullUrl)) {
                                        urls.push(fullUrl);
                                    }
                                }
                            }
                        }
                        return urls;
                    }
                """)
                
                # Add to set (auto-deduplicates)
                prev_count = len(all_profile_urls)
                all_profile_urls.update(current_urls)
                new_count = len(all_profile_urls)
                
                if new_count > prev_count:
                    print(f"[INFO] Found {new_count} total connection profiles...")
                    no_new_profiles_count = 0
                else:
                    no_new_profiles_count += 1
                
                # Get current scroll height
                current_height = await self.browser_controller.page.evaluate("document.body.scrollHeight")
                
                # If no new profiles found in 8 consecutive scrolls AND height not changing, we've reached the end
                if no_new_profiles_count >= 8 and current_height == last_height:
                    print(f"[INFO] Reached end of connections list. Total: {len(all_profile_urls)}")
                    break
                
                last_height = current_height
                
                # Scroll down with multiple methods for better loading
                await self.browser_controller.page.evaluate("""
                    () => {
                        // Method 1: Scroll to bottom
                        window.scrollTo(0, document.body.scrollHeight);
                        
                        // Method 2: Also scroll the main container if exists
                        const containers = document.querySelectorAll('.scaffold-finite-scroll__content, .mn-connections');
                        for (const container of containers) {
                            container.scrollTop = container.scrollHeight;
                        }
                    }
                """)
                
                # Wait for content to load - longer delay for lazy loading
                await human_behavior.random_delay(2, 4)
                
                # Every 5 scrolls, do a small scroll up then down to trigger loading
                if scroll_attempts % 5 == 0 and scroll_attempts > 0:
                    await self.browser_controller.page.evaluate("window.scrollBy(0, -300)")
                    await asyncio.sleep(0.5)
                    await self.browser_controller.page.evaluate("window.scrollBy(0, 500)")
                    await asyncio.sleep(1)
                
                scroll_attempts += 1
            
            # Convert set to list and preserve order
            profile_urls = list(all_profile_urls)
            
            print(f"\n[INFO] ===== TOTAL CONNECTIONS FOUND: {len(profile_urls)} =====")
            logger.info(f"[INFO] Total connections found after scrolling: {len(profile_urls)}")
            
            if not profile_urls:
                logger.warning("[X] No connection profiles found")
                print("[WARNING] No connection profiles found")
                return
            
            # ===== FILTER OUT ALREADY SCRAPED PROFILES =====
            print("[INFO] Checking database for already scraped profiles...")
            unscraped_urls = self.db.get_unscraped_profiles(profile_urls)
            
            already_scraped = len(profile_urls) - len(unscraped_urls)
            if already_scraped > 0:
                print(f"[INFO] Skipping {already_scraped} already scraped profiles")
                logger.info(f"[INFO] Skipping {already_scraped} already scraped profiles")
            
            print(f"[INFO] {len(unscraped_urls)} profiles available for scraping")
            
            if not unscraped_urls:
                print("[INFO] All connections have already been scraped!")
                logger.info("[INFO] All connections already scraped")
                
                # Still export existing data
                all_profiles = self.db.get_all_scraped_data()
                if all_profiles:
                    self.exporter.export_all_formats(all_profiles)
                    print(f"[OK] Exported {len(all_profiles)} existing profiles")
                return
            
            # Limit to requested number from unscraped
            profiles_to_scrape = unscraped_urls[:max_profiles]
            print(f"[INFO] Will scrape {len(profiles_to_scrape)} profiles (requested: {max_profiles})")
            logger.info(f"[INFO] Will scrape {len(profiles_to_scrape)} profiles")
            
            # ===== CREATE HISTORY ENTRY =====
            history_id = self.db.create_scraping_history(
                session_type='connections',
                query_or_source='My Connections',
                total_found=len(profile_urls),
                total_requested=max_profiles
            )
            
            # Add all found profiles to database queue (for resume capability)
            self.db.add_profiles(unscraped_urls)
            self.db.add_profiles_to_queue(history_id, profiles_to_scrape)
            
            # Scrape each profile using scrape_agent
            print("[INFO] Starting profile scraping...")
            logger.info("[INFO] Starting profile scraping...")
            
            scrape_results = await self.scrape_agent.scrape_multiple_profiles(
                profiles_to_scrape,
                delay_range=self.config.scraping['delay_between_profiles']
            )
            
            print(f"[INFO] Scraped {len(scrape_results['profiles'])} profiles")
            logger.info(f"[INFO] Scraped {len(scrape_results['profiles'])} profiles")
            
            # Save to database and update queue status
            print("[INFO] Saving profiles to database...")
            scraped_urls = set()
            for profile_data in scrape_results['profiles']:
                if profile_data:
                    profile_url = profile_data.get('profile_url', '')
                    completeness = profile_data.get('completeness', 0.5)
                    self.db.save_profile_data(profile_url, profile_data, completeness)
                    self.db.update_queue_status(history_id, profile_url, 'completed')
                    scraped_urls.add(profile_url)
            
            # Mark failed profiles
            for url in profiles_to_scrape:
                if url not in scraped_urls:
                    self.db.mark_profile_failed(url, "Scraping failed or blocked")
                    self.db.update_queue_status(history_id, url, 'failed')
            
            # Update history stats
            self.db.update_history_stats(history_id)
            
            # Validate scraped data
            if scrape_results['profiles']:
                print("[INFO] Validating scraped data...")
                logger.info("[INFO] Validating scraped data...")
                validation_results = self.validation_agent.batch_validate(scrape_results['profiles'])
                
                logger.info(f"[OK] Validation: {validation_results['valid']}/{validation_results['total']} valid")
                logger.info(f"[STAT] Avg Completeness: {validation_results['avg_completeness']}%")
                logger.info(f"[STAT] Avg Score: {validation_results['avg_score']}/100")
                
                # Export data - use both DB data and newly scraped data
                print("[INFO] Exporting data...")
                logger.info(f"\n{'='*60}")
                logger.info("EXPORTING DATA")
                logger.info(f"{'='*60}\n")
                
                # Get all profiles from DB (should include our newly saved ones)
                all_profiles = self.db.get_all_scraped_data()
                
                # If DB is empty, export the scraped profiles directly
                if not all_profiles:
                    logger.warning("No profiles in database, exporting scraped profiles directly...")
                    all_profiles = scrape_results['profiles']
                else:
                    # Add any newly scraped profiles that might not be in DB yet
                    existing_urls = {p.get('profile_url') for p in all_profiles}
                    for profile in scrape_results['profiles']:
                        if profile and profile.get('profile_url') not in existing_urls:
                            all_profiles.append(profile)
                
                if all_profiles:
                    export_results = self.exporter.export_all_formats(all_profiles)
                    
                    for format_name, success in export_results.items():
                        if success:
                            logger.info(f"[OK] Exported to {format_name.upper()}")
                            print(f"[OK] Exported to {format_name.upper()}")
                else:
                    logger.error("No profiles to export")
                    print("[ERROR] No profiles to export")
            
            # Final statistics
            final_stats = self.db.get_scraping_stats()
            logger.info(f"\n{'='*60}")
            logger.info("FINAL STATISTICS")
            logger.info(f"{'='*60}")
            logger.info(f"Total Profiles: {final_stats['total']}")
            logger.info(f"Completed: {final_stats['completed']}")
            logger.info(f"Failed: {final_stats['failed']}")
            logger.info(f"Pending: {final_stats['pending']}")
            logger.info(f"Success Rate: {final_stats['success_rate']}")
            logger.info(f"Avg Completeness: {final_stats['avg_completeness']}")
            logger.info(f"Database Size: {self.db.get_db_size()}")
            logger.info(f"Export Path: {self.exporter.get_export_path()}")
            logger.info(f"{'='*60}\n")
            
            print("[SUCCESS] Connection scraping workflow completed!")
            
        except Exception as e:
            logger.error(f"[X] Connections scraping error: {e}")
            print(f"[ERROR] Connections scraping error: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def workflow_export(self):
        """Export existing data"""
        try:
            logger.info("\n📤 EXPORTING EXISTING DATA")
            logger.info(f"{'='*60}\n")
            
            # Get all profiles from database
            profiles = self.db.get_all_scraped_data(
                min_completeness=self.config.export['min_completeness']
            )
            
            if not profiles:
                logger.warning("No profiles to export")
                return
            
            logger.info(f"Exporting {len(profiles)} profiles...")
            
            results = self.exporter.export_all_formats(profiles)
            
            for format_name, success in results.items():
                if success:
                    logger.info(f"[OK] Exported to {format_name.upper()}")
            
            logger.info(f"\n[OK] Export completed: {self.exporter.get_export_path()}")
            
        except Exception as e:
            logger.error(f"[X] Export error: {e}")
    
    async def show_menu(self) -> int:
        """Show interactive menu"""
        print("\n" + "="*60)
        print("[MENU] SELECT MODE")
        print("="*60)
        print("1. Search & Scrape New Profiles")
        print("2. Scrape My Connections")
        print("3. Resume Previous Scraping")
        print("4. Export Existing Data")
        print("5. View Statistics")
        print("6. Cleanup Old Data")
        print("7. Retry Failed Profiles")
        print("0. Exit")
        print("="*60)
        
        while True:
            try:
                choice = input("\nEnter your choice (0-7): ").strip()
                if choice in ['0', '1', '2', '3', '4', '5', '6', '7']:
                    return int(choice)
                print("[X] Invalid choice. Please try again.")
            except KeyboardInterrupt:
                return 0
    
    async def show_statistics(self):
        """Show detailed statistics with history"""
        print("\n" + "="*60)
        print("[STAT] DATABASE STATISTICS")
        print("="*60)
        
        stats = self.db.get_scraping_stats()
        for key, value in stats.items():
            print(f"  {key.replace('_', ' ').title()}: {value}")
        
        # Show scraping history
        print("\n" + "-"*60)
        print("[HISTORY] RECENT SCRAPING SESSIONS")
        print("-"*60)
        
        history = self.db.get_scraping_history(limit=15)
        if history:
            for h in history:
                status_icon = "✓" if h['status'] == 'completed' else "⏳" if h['status'] == 'active' else "✗"
                print(f"  {status_icon} #{h['id']} [{h['type']:<12}] {h['query'][:25]:<25} | "
                      f"Found: {h['total_found']:<4} Req: {h['requested']:<4} "
                      f"Done: {h['scraped']:<4} Pend: {h['pending']:<4} Fail: {h['failed']:<3}")
        else:
            print("  No scraping history yet.")
        
        # Failed profiles
        failed = self.db.get_failed_profiles()
        if failed:
            print(f"\n[X] Failed Profiles ({len(failed)}):")
            for profile in failed[:10]:
                print(f"   • {profile['url'][-40:]}: {profile['error'][:40] if profile['error'] else 'Unknown'}")
        
        print("="*60 + "\n")
    
    async def cleanup_data(self):
        """Cleanup old data with options"""
        print("\n🧹 CLEANUP OPTIONS:")
        print("  1. Delete failed profiles older than X days")
        print("  2. Clear all scraping history")
        print("  3. Reset pending profiles to retry")
        print("  4. Clear everything (fresh start)")
        print("  0. Cancel")
        
        try:
            choice = input("\nSelect option (0-4): ").strip()
            
            if choice == '1':
                days = int(input("Delete data older than (days): ") or "30")
                deleted = self.db.cleanup_old_data(days)
                print(f"[OK] Deleted {deleted} old failed records")
                
            elif choice == '2':
                confirm = input("Clear all scraping history? (yes/no): ").strip().lower()
                if confirm == 'yes':
                    conn = self.db._get_connection()
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM scraping_history")
                    cursor.execute("DELETE FROM profile_queue")
                    conn.commit()
                    conn.close()
                    print("[OK] Scraping history cleared")
                    
            elif choice == '3':
                conn = self.db._get_connection()
                cursor = conn.cursor()
                cursor.execute("UPDATE profiles SET status = 'pending', retry_count = 0 WHERE status = 'failed' AND retry_count < 5")
                updated = cursor.rowcount
                conn.commit()
                conn.close()
                print(f"[OK] Reset {updated} failed profiles to pending")
                
            elif choice == '4':
                confirm = input("⚠️ DELETE EVERYTHING? This cannot be undone! (type 'DELETE'): ").strip()
                if confirm == 'DELETE':
                    conn = self.db._get_connection()
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM profiles")
                    cursor.execute("DELETE FROM scraping_history")
                    cursor.execute("DELETE FROM profile_queue")
                    cursor.execute("DELETE FROM search_sessions")
                    cursor.execute("DELETE FROM quality_logs")
                    cursor.execute("VACUUM")
                    conn.commit()
                    conn.close()
                    print("[OK] All data cleared. Fresh start!")
                else:
                    print("[CANCELLED]")
            else:
                print("[CANCELLED]")
                
        except Exception as e:
            print(f"[ERROR] Cleanup error: {e}")
    
    async def run(self):
        """Run main application loop"""
        try:
            self.start_time = datetime.now()
            
            # Show banner
            print_banner()
            
            # Show configuration
            print_config_info(self.config)
            
            # Initialize
            if not await self.initialize():
                logger.error("Initialization failed")
                return
            
            # Login
            if not await self.login():
                logger.error("Login failed")
                return
            
            # Main loop
            while True:
                choice = await self.show_menu()
                
                if choice == 0:
                    break
                elif choice == 1:
                    # Search & Scrape
                    queries = input("\nEnter search queries (comma-separated): ").split(',')
                    queries = [q.strip() for q in queries if q.strip()]
                    
                    if queries:
                        max_profiles = int(input("Max profiles per query (default 50): ") or "50")
                        await self.workflow_search_and_scrape(queries, max_profiles)
                
                elif choice == 2:
                    # Scrape Connections
                    max_profiles = int(input("Max connection profiles to scrape (default 50): ") or "50")
                    print("[WAIT] Starting connection scraping workflow...")
                    await self.workflow_scrape_connections(max_profiles)
                    print("[DONE] Workflow completed, returning to menu")
                
                elif choice == 3:
                    # Resume - Show pending count first
                    stats = self.db.get_scraping_stats()
                    pending_count = stats.get('pending', 0)
                    
                    if pending_count == 0:
                        print("\n[INFO] ✓ No pending profiles to resume!")
                        print("[TIP] Use option 1 or 2 to find new profiles to scrape.")
                        continue
                    
                    print(f"\n[INFO] 📋 Pending profiles: {pending_count}")
                    limit = int(input(f"How many to resume (max {pending_count}, default 100): ") or "100")
                    limit = min(limit, pending_count)  # Don't exceed available
                    await self.workflow_resume(limit)
                
                elif choice == 4:
                    # Export
                    await self.workflow_export()
                
                elif choice == 5:
                    # Statistics
                    await self.show_statistics()
                
                elif choice == 6:
                    # Cleanup
                    await self.cleanup_data()
                
                elif choice == 7:
                    # Retry Failed - Show failed count first
                    stats = self.db.get_scraping_stats()
                    failed_count = stats.get('failed', 0)
                    
                    if failed_count == 0:
                        print("\n[INFO] ✓ No failed profiles to retry!")
                        continue
                    
                    print(f"\n[INFO] ❌ Failed profiles: {failed_count}")
                    limit = int(input(f"How many to retry (max {failed_count}, default 50): ") or "50")
                    limit = min(limit, failed_count)  # Don't exceed available
                    await self.workflow_retry_failed(limit)
            
            logger.info("\n[OK] Goodbye!")
            
        except KeyboardInterrupt:
            logger.info("\n[INTERRUPT] Interrupted by user")
        except Exception as e:
            logger.error(f"[X] Fatal error: {e}")
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Cleanup and shutdown"""
        logger.info("[SHUTDOWN] Shutting down...")
        
        if self.browser_controller:
            await self.browser_controller.cleanup()
        
        if self.start_time:
            elapsed = datetime.now() - self.start_time
            logger.info(f"Total execution time: {elapsed}")
        
        logger.info("[OK] Shutdown completed")


async def main():
    """Main entry point"""
    # Create directories
    Path('logs').mkdir(exist_ok=True)
    Path('data/exports').mkdir(parents=True, exist_ok=True)
    Path('config').mkdir(exist_ok=True)
    
    # Setup logging
    setup_logging(level=os.getenv('LOG_LEVEL', 'INFO'))
    
    # Run application
    app = LinkedInScraperApp()
    await app.run()


if __name__ == "__main__":
    # Load environment variables
    from pathlib import Path
    env_file = Path('.env')
    if env_file.exists():
        import dotenv
        try:
            dotenv.load_dotenv(env_file)
        except:
            # dotenv not installed, skip
            pass
    
    # Run async main
    asyncio.run(main())
