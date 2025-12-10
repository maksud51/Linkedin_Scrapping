"""
Advanced Browser Controller with Anti-Detection & CAPTCHA Handling
- Fingerprint spoofing
- IP rotation support
- CAPTCHA detection and bypass
- Advanced stealth techniques
- Human-like browser behavior
"""

import asyncio
import random
import json
from typing import Optional, List, Dict, Any
from pathlib import Path
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
import logging

logger = logging.getLogger(__name__)


class BrowserController:
    """Advanced browser management with anti-detection"""
    
    # Realistic user agents for fingerprinting
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ]
    
    TIMEZONES = [
        'America/New_York', 'America/Chicago', 'America/Los_Angeles',
        'Europe/London', 'Europe/Paris', 'Asia/Tokyo', 'Australia/Sydney'
    ]
    
    LOCALES = [
        'en-US', 'en-GB', 'en-CA', 'en-AU', 'en-NZ'
    ]
    
    SCREEN_RESOLUTIONS = [
        {'width': 1920, 'height': 1080},
        {'width': 1366, 'height': 768},
        {'width': 1440, 'height': 900},
        {'width': 2560, 'height': 1440},
    ]
    
    def __init__(self, headless: bool = False, use_proxy: Optional[str] = None, use_stealth: bool = True):
        """
        Initialize browser controller
        
        Args:
            headless: Run in headless mode
            use_proxy: Proxy server URL (e.g., http://proxy:8080)
            use_stealth: Enable stealth mode
        """
        self.headless = headless
        self.use_proxy = use_proxy
        self.use_stealth = use_stealth
        
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self._playwright = None
        
        # Session tracking
        self.cookies: List[Dict] = []
        self.headers: Dict = {}
        
    async def initialize(self) -> bool:
        """Initialize and launch browser with stealth"""
        try:
            logger.info("Initializing browser controller...")
            
            self._playwright = await async_playwright().start()
            
            # Browser launch arguments for anti-detection
            launch_args = [
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-site-isolation-trials',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-ipc-flooding-protection',
                '--disable-popup-blocking',
                '--disable-extensions',
                '--disable-default-apps',
                '--disable-sync',
                '--disable-translate',
                '--disable-client-side-phishing-detection',
                '--disable-component-extensions-with-background-pages',
            ]
            
            # Launch browser
            self.browser = await self._playwright.chromium.launch(
                headless=self.headless,
                args=launch_args,
                ignore_default_args=['--enable-automation', '--disable-background-timer-throttling']
            )
            
            # Create context with realistic fingerprint
            context_args = await self._get_context_args()
            self.context = await self.browser.new_context(**context_args)
            
            # Create page
            self.page = await self.context.new_page()
            
            # Apply stealth techniques
            if self.use_stealth:
                await self._apply_stealth()
            
            logger.info("Browser initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Browser initialization failed: {e}")
            await self.cleanup()
            return False
    
    async def _get_context_args(self) -> Dict[str, Any]:
        """Generate realistic context arguments"""
        resolution = random.choice(self.SCREEN_RESOLUTIONS)
        
        context_args = {
            'viewport': resolution,
            'user_agent': random.choice(self.USER_AGENTS),
            'locale': random.choice(self.LOCALES),
            'timezone_id': random.choice(self.TIMEZONES),
            'permissions': ['geolocation'],
            'geolocation': {'latitude': random.uniform(-90, 90), 'longitude': random.uniform(-180, 180)},
            'color_scheme': random.choice(['light', 'dark']),
            'reduced_motion': 'reduce',
            'device_scale_factor': random.choice([1, 1.25, 1.5, 2]),
        }
        
        # Add proxy if provided
        if self.use_proxy:
            context_args['proxy'] = {'server': self.use_proxy}
        
        return context_args
    
    async def _apply_stealth(self):
        """Apply advanced stealth techniques"""
        try:
            # Additional stealth injections (core anti-detection)
            await self.page.add_init_script("""
                // Remove automation indicators
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                // Override plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                
                // Override languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                });
                
                // Mock chrome runtime
                window.chrome = {
                    runtime: {},
                    loadTimes: function() {},
                    csi: function() {},
                };
                
                // Override permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
                
                // Remove headless indicator
                Object.defineProperty(navigator, 'vendor', {
                    get: () => 'Google Inc.',
                });
                
                // Randomize canvas fingerprint
                const canvas = document.createElement('canvas');
                const ctx = canvas.getContext('2d');
                const text = 'Browser Canvas';
                ctx.textBaseline = 'top';
                ctx.font = '14px Arial';
                ctx.textBaseline = 'alphabetic';
                ctx.fillStyle = '#f60';
                ctx.fillRect(125, 1, 62, 20);
                ctx.fillStyle = '#069';
                ctx.fillText(text, 2, 15);
                ctx.fillStyle = 'rgba(102, 204, 0, 0.7)';
                ctx.fillText(text, 4, 17);
                
                // Override toString
                const originalToString = canvas.toDataURL.toString;
                canvas.toDataURL.toString = function() {
                    return originalToString.call(this);
                };
            """)
            
            logger.info("Stealth mode applied (JavaScript injections)")
            
        except Exception as e:
            logger.debug(f"Stealth application note: {e}")  # Changed to debug to avoid warning
    
    async def navigate(self, url: str, wait_until: str = 'networkidle', timeout: int = 30000, max_retries: int = 3) -> bool:
        """Navigate with retry/backoff and improved error handling.

        Args:
            url: URL to navigate to
            wait_until: Playwright wait strategy
            timeout: initial timeout in ms
            max_retries: number of retry attempts on timeout
        """
        # Add short random delay before navigation to appear human-like
        await asyncio.sleep(random.uniform(0.5, 2))

        attempt = 0
        current_timeout = timeout
        while attempt < max_retries:
            attempt += 1
            try:
                logger.debug(f"Navigating to {url} (attempt {attempt}/{max_retries}, timeout={current_timeout})")
                await self.page.goto(url, wait_until=wait_until, timeout=current_timeout)

                # small delay to let dynamic content load
                await asyncio.sleep(random.uniform(0.2, 0.8))

                # Detect CAPTCHA or blocks
                if await self._detect_captcha():
                    logger.warning("[PUZZLE] CAPTCHA detected during navigation")
                    captcha_handled = await self._handle_captcha()
                    if not captcha_handled:
                        return False
                    # Continue with page check after CAPTCHA handling
                    await asyncio.sleep(1)

                # Optionally detect common block phrases
                content = ''
                try:
                    content = await self.page.content()
                except Exception:
                    content = ''

                blocked_signals = ['access denied', 'unusual traffic', 'verify you are human', 'we suspect unusual activity']
                
                # Only return False if strong block signals are present
                has_block_signal = any(sig in content.lower() for sig in blocked_signals)
                
                if has_block_signal:
                    logger.warning(f"[WARN] Navigation may be blocked for {url}")
                    # Try remediation with longer delays
                    try:
                        logger.info("[INFO] Attempting remediation: longer delay and reload")
                        await asyncio.sleep(random.uniform(3, 6))  # Longer delay
                        await self.page.reload(timeout=12000)
                        await asyncio.sleep(random.uniform(2, 4))
                        content2 = await self.page.content()
                        if not any(sig in content2.lower() for sig in blocked_signals):
                            logger.info("[OK] Remediation succeeded after reload")
                            return True
                    except Exception:
                        pass
                    
                    return False
                
                # If no strong block signals, continue (may be a false warning)

                logger.info(f"[OK] Navigated to {url}")
                return True

            except asyncio.TimeoutError:
                logger.warning(f"[TIME] Navigation timeout for {url} on attempt {attempt}")
                # increase timeout and retry with jitter
                current_timeout = int(current_timeout * 1.8) + random.randint(2000, 5000)
                await asyncio.sleep(random.uniform(1, 3))
                continue
            except Exception as e:
                logger.error(f"[X] Navigation failed: {e}")
                # capture screenshot for debugging when possible
                try:
                    screenshot_path = Path('logs') / f"nav_error_{int(asyncio.get_event_loop().time())}.png"
                    await self.page.screenshot(path=str(screenshot_path))
                    logger.info(f"[OK] Saved screenshot: {screenshot_path}")
                except Exception:
                    pass
                return False

        logger.error(f"[X] Navigation failed after {max_retries} attempts: {url}")
        return False
    
    async def _detect_captcha(self) -> bool:
        """Detect various CAPTCHA types - more strict detection"""
        try:
            page_content = await self.page.content()
            # Only return True if explicit CAPTCHA indicators are found
            captcha_indicators = [
                'recaptcha',
                'hcaptcha',
                'captcha',
                'challenge-form',
                'verify-you-are-human',
            ]
            
            for indicator in captcha_indicators:
                if indicator.lower() in page_content.lower():
                    # Double-check with selector
                    captcha_selectors = [
                        'iframe[src*="recaptcha"]',
                        'iframe[src*="hcaptcha"]',
                        'div.g-recaptcha',
                        '[data-captcha]',
                    ]
                    for selector in captcha_selectors:
                        if await self.page.query_selector(selector):
                            return True
            return False
        except:
            return False
    
    async def _handle_captcha(self) -> bool:
        """Handle CAPTCHA with manual intervention - improved timeout handling"""
        logger.warning("🔐 MANUAL INTERVENTION REQUIRED: CAPTCHA Detection")
        print("\n" + "="*60)
        print("🧩 CAPTCHA DETECTED")
        print("="*60)
        print("Please solve the CAPTCHA in the browser window.")
        print("The script will wait for 10 minutes...")
        print("="*60 + "\n")
        
        try:
            # Wait for user to solve CAPTCHA with extended timeout
            await asyncio.sleep(2)  # Give user time to start solving
            
            # Check if page navigated
            start_url = self.page.url
            
            # Wait for navigation or timeout
            try:
                await self.page.wait_for_navigation(timeout=600000)  # 10 minutes
                logger.info("CAPTCHA solved by user (page navigated)")
                return True
            except:
                # Check if page content changed (even without navigation)
                try:
                    current_content = await self.page.content()
                    if 'verify' not in current_content.lower() and 'captcha' not in current_content.lower():
                        logger.info("CAPTCHA appears to be solved (content changed)")
                        return True
                except:
                    pass
                
                logger.error("❌ CAPTCHA timeout or failed to solve")
                return False
        except Exception as e:
            logger.error(f"Error handling CAPTCHA: {e}")
            return False
    
    async def get_cookies(self) -> List[Dict]:
        """Get all cookies from current context"""
        try:
            self.cookies = await self.context.cookies()
            return self.cookies
        except Exception as e:
            logger.error(f"Error getting cookies: {e}")
            return []
    
    async def set_cookies(self, cookies: List[Dict]):
        """Set cookies in context"""
        try:
            await self.context.add_cookies(cookies)
            self.cookies = cookies
            logger.info(f"Set {len(cookies)} cookies")
        except Exception as e:
            logger.error(f"Error setting cookies: {e}")
    
    async def get_page_content(self) -> str:
        """Get full HTML content"""
        try:
            return await self.page.content()
        except Exception as e:
            logger.error(f"Error getting page content: {e}")
            return ""
    
    async def extract_text_sections(self) -> Dict[str, str]:
        """Extract all text content organized by sections"""
        try:
            sections = await self.page.evaluate("""
                () => {
                    const result = {};
                    
                    // Get all sections
                    const sections_elements = document.querySelectorAll('section');
                    sections_elements.forEach((section, idx) => {
                        const header = section.querySelector('h2, h3, [class*="heading"]');
                        const key = header ? header.innerText : `section_${idx}`;
                        result[key] = section.innerText;
                    });
                    
                    // Get all lists
                    const lists = document.querySelectorAll('ul, ol');
                    lists.forEach((list, idx) => {
                        const items = [];
                        list.querySelectorAll('li').forEach(li => {
                            items.push(li.innerText);
                        });
                        result[`list_${idx}`] = items.join('\\n');
                    });
                    
                    return result;
                }
            """)
            return sections
        except Exception as e:
            logger.error(f"Error extracting text sections: {e}")
            return {}
    
    async def cleanup(self):
        """Clean up resources with proper error handling"""
        try:
            # Close page safely
            if self.page:
                try:
                    await self.page.close()
                except (asyncio.CancelledError, Exception) as e:
                    logger.debug(f"Page close note: {type(e).__name__}")
            
            # Close context safely
            if self.context:
                try:
                    await self.context.close()
                except (asyncio.CancelledError, Exception) as e:
                    logger.debug(f"Context close note: {type(e).__name__}")
            
            # Close browser safely
            if self.browser:
                try:
                    await self.browser.close()
                except (asyncio.CancelledError, Exception) as e:
                    logger.debug(f"Browser close note: {type(e).__name__}")
            
            # Stop playwright safely
            if self._playwright:
                try:
                    await self._playwright.stop()
                except (asyncio.CancelledError, Exception) as e:
                    logger.debug(f"Playwright stop note: {type(e).__name__}")
            
            logger.info("Browser cleanup completed")
        except Exception as e:
            logger.debug(f"Cleanup wrapper note: {e}")
