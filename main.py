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
        """Complete workflow: Search → Scrape → Validate → Export"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info("WORKFLOW: SEARCH AND SCRAPE")
            logger.info(f"{'='*60}\n")
            
            session_id = self.db.create_search_session(f"batch_{len(search_queries)}_queries")
            
            all_profiles = []
            
            for query_idx, query in enumerate(search_queries, 1):
                logger.info(f"\nQuery {query_idx}/{len(search_queries)}: '{query}'")
                logger.info("-" * 60)
                
                # Search profiles
                logger.info("Searching for profiles...")
                profile_urls = await self.search_agent.search_profiles(
                    query,
                    max_results=min(max_profiles_per_query, self.config.scraping['max_profiles_per_search'])
                )
                
                if not profile_urls:
                    logger.warning(f"No profiles found for query: {query}")
                    continue
                
                # Add to database
                added = self.db.add_profiles(profile_urls, session_id)
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
                        scraped_urls.add(profile_url)

                # Mark any profile URLs that were not scraped as failed (increase retry count)
                for url in profile_urls:
                    if url not in scraped_urls:
                        self.db.mark_profile_failed(url, "Navigation/Access failed or blocked")
                
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
        """Resume scraping from last checkpoint"""
        try:
            logger.info("RESUMING SCRAPING FROM CHECKPOINT")
            logger.info(f"{'='*60}\n")
            
            pending = self.db.get_pending_profiles(limit)
            
            if not pending:
                logger.info("[OK] No pending profiles to resume")
                return
            
            logger.info(f"Found {len(pending)} pending profiles")
            
            # Scrape
            results = await self.scrape_agent.scrape_multiple_profiles(
                pending,
                delay_range=self.config.scraping['delay_between_profiles']
            )
            
            # Validate
            validation_results = self.validation_agent.batch_validate(results['profiles'])
            logger.info(f"[OK] Validation: {validation_results['valid']}/{validation_results['total']} valid")
            
            # Export
            self.exporter.export_all_formats(results['profiles'])
            
            # Stats
            final_stats = self.db.get_scraping_stats()
            logger.info(f"\nFinal Statistics: {final_stats}")
            
        except Exception as e:
            logger.error(f"[X] Resume error: {e}")
    
    async def workflow_scrape_connections(self, max_profiles: int = 50):
        """Scrape profiles from user's connections"""
        print(f"\n[DEBUG] workflow_scrape_connections called with max_profiles={max_profiles}")
        try:
            logger.info(f"\n{'='*60}")
            logger.info("SCRAPING CONNECTIONS PROFILES")
            logger.info(f"{'='*60}\n")
            
            # Instead of complex connections agent, use browser to navigate directly
            print("[INFO] Navigating to LinkedIn connections page...")
            logger.info("[INFO] Navigating to LinkedIn connections page...")
            
            # Navigate to connections
            connections_url = 'https://www.linkedin.com/mynetwork/invite-connect/connections/'
            if not await self.browser_controller.navigate(connections_url, wait_until='domcontentloaded', timeout=30000):
                logger.error("[X] Failed to navigate to connections page")
                print("[ERROR] Failed to navigate to connections page")
                return
            
            print("[INFO] Waiting for page to load...")
            await asyncio.sleep(3)
            
            # Extract connection profile URLs from the page
            print("[INFO] Extracting profile URLs from connections...")
            logger.info("[INFO] Extracting profile URLs from connections...")
            
            profile_urls = await self.browser_controller.page.evaluate("""
                () => {
                    const urls = [];
                    const links = document.querySelectorAll('a[href*="/in/"]');
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
                    return urls.slice(0, 100);
                }
            """)
            
            print(f"[INFO] Found {len(profile_urls)} connection profiles")
            logger.info(f"[INFO] Found {len(profile_urls)} connection profiles")
            
            if not profile_urls:
                logger.warning("[X] No connection profiles found")
                print("[WARNING] No connection profiles found")
                return
            
            # Limit to requested number
            profile_urls = profile_urls[:max_profiles]
            print(f"[INFO] Will scrape {len(profile_urls)} profiles")
            logger.info(f"[INFO] Will scrape {len(profile_urls)} profiles")
            
            # Scrape each profile using scrape_agent
            print("[INFO] Starting profile scraping...")
            logger.info("[INFO] Starting profile scraping...")
            
            scrape_results = await self.scrape_agent.scrape_multiple_profiles(
                profile_urls,
                delay_range=self.config.scraping['delay_between_profiles']
            )
            
            print(f"[INFO] Scraped {len(scrape_results['profiles'])} profiles")
            logger.info(f"[INFO] Scraped {len(scrape_results['profiles'])} profiles")
            
            # Save to database
            print("[INFO] Saving profiles to database...")
            for profile_data in scrape_results['profiles']:
                if profile_data:
                    profile_url = profile_data.get('profile_url', '')
                    completeness = profile_data.get('completeness', 0.5)
                    self.db.save_profile_data(profile_url, profile_data, completeness)
            
            # Validate scraped data
            if scrape_results['profiles']:
                print("[INFO] Validating scraped data...")
                logger.info("[INFO] Validating scraped data...")
                validation_results = self.validation_agent.batch_validate(scrape_results['profiles'])
                
                logger.info(f"[OK] Validation: {validation_results['valid']}/{validation_results['total']} valid")
                logger.info(f"[STAT] Avg Completeness: {validation_results['avg_completeness']}%")
                logger.info(f"[STAT] Avg Score: {validation_results['avg_score']}/100")
                
                # Export data
                print("[INFO] Exporting data...")
                logger.info(f"\n{'='*60}")
                logger.info("EXPORTING DATA")
                logger.info(f"{'='*60}\n")
                
                all_profiles = self.db.get_all_scraped_data()
                export_results = self.exporter.export_all_formats(all_profiles)
                
                for format_name, success in export_results.items():
                    if success:
                        logger.info(f"[OK] Exported to {format_name.upper()}")
                        print(f"[OK] Exported to {format_name.upper()}")
            
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
        print("0. Exit")
        print("="*60)
        
        while True:
            try:
                choice = input("\nEnter your choice (0-6): ").strip()
                if choice in ['0', '1', '2', '3', '4', '5', '6']:
                    return int(choice)
                print("[X] Invalid choice. Please try again.")
            except KeyboardInterrupt:
                return 0
    
    async def show_statistics(self):
        """Show detailed statistics"""
        logger.info("\n" + "="*60)
        logger.info("[STAT] DATABASE STATISTICS")
        logger.info("="*60)
        
        stats = self.db.get_scraping_stats()
        for key, value in stats.items():
            logger.info(f"{key.replace('_', ' ').title()}: {value}")
        
        # Failed profiles
        failed = self.db.get_failed_profiles()
        if failed:
            logger.info(f"\n[X] Failed Profiles ({len(failed)}):")
            for profile in failed[:10]:
                logger.info(f"   • {profile['url']}: {profile['error'][:50]}")
        
        logger.info("="*60 + "\n")
    
    async def cleanup_data(self):
        """Cleanup old data"""
        logger.info("\n🧹 CLEANING UP OLD DATA")
        days = int(input("Delete data older than (days): "))
        deleted = self.db.cleanup_old_data(days)
        logger.info(f"[OK] Deleted {deleted} old records")
    
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
                    # Resume
                    limit = int(input("How many profiles to resume (default 100): ") or "100")
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
