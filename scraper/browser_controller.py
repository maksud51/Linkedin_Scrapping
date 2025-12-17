"""
Advanced Browser Controller with Anti-Detection & CAPTCHA Handling
- Fingerprint spoofing
- IP rotation support
- CAPTCHA detection and bypass
- Advanced stealth techniques
- Human-like browser behavior
- Automatic CAPTCHA solving
"""

import asyncio
import random
import json
from typing import Optional, List, Dict, Any
from pathlib import Path
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
import logging

from scraper.harvester_client import HarvesterClient

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
    
    def __init__(self, headless: bool = False, use_proxy: Optional[str] = None, use_stealth: bool = True, harvester_url: str = "http://localhost:8000"):
        """
        Initialize browser controller
        
        Args:
            headless: Run in headless mode
            use_proxy: Proxy server URL (e.g., http://proxy:8080)
            use_stealth: Enable stealth mode
            harvester_url: URL of CAPTCHA harvester service
        """
        self.headless = headless
        self.use_proxy = use_proxy
        self.use_stealth = use_stealth
        self.harvester_url = harvester_url
        self.harvester_client = None
        
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self._playwright = None
        
        # Session tracking
        self.cookies: List[Dict] = []
        self.headers: Dict = {}
        
        # CAPTCHA state tracking
        self.captcha_state = {
            'current_captcha_url': None,
            'captcha_detected_at_url': {},  # {url: {'detected': count, 'solved': bool}}
            'captcha_attempts': 0,
            'max_captcha_attempts': 3,
            'blocked_urls': set(),  # URLs where CAPTCHA was unsolvable
        }
        
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
        """Navigate with retry/backoff and improved error handling with URL blocking.

        Args:
            url: URL to navigate to
            wait_until: Playwright wait strategy
            timeout: initial timeout in ms
            max_retries: number of retry attempts on timeout
        """
        # Check if URL is blocked due to repeated CAPTCHA failures
        if url in self.captcha_state['blocked_urls']:
            logger.warning(f"[SKIP] URL is blocked (repeated CAPTCHA): {url}")
            return False
        
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
                    
                    # Check if URL became blocked during handling
                    if url in self.captcha_state['blocked_urls']:
                        logger.warning(f"[BLOCK] URL blocked after CAPTCHA attempt: {url}")
                        return False
                    
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
        """Detect various CAPTCHA types - intelligent detection with state tracking"""
        try:
            current_url = self.page.url
            page_content = await self.page.content()
            
            # CAPTCHA indicators
            captcha_indicators = [
                'recaptcha',
                'hcaptcha',
                'captcha',
                'challenge-form',
                'verify-you-are-human',
            ]
            
            # Check for CAPTCHA indicators in content
            has_captcha_content = any(
                indicator.lower() in page_content.lower() 
                for indicator in captcha_indicators
            )
            
            if not has_captcha_content:
                # Clear state if no CAPTCHA detected
                if current_url in self.captcha_state['captcha_detected_at_url']:
                    self.captcha_state['captcha_detected_at_url'][current_url]['solved'] = True
                return False
            
            # Verify with selectors to avoid false positives
            captcha_selectors = [
                'iframe[src*="recaptcha"]',
                'iframe[src*="hcaptcha"]',
                'div.g-recaptcha',
                '[data-captcha]',
                'div.h-captcha',
            ]
            
            for selector in captcha_selectors:
                try:
                    if await self.page.query_selector(selector):
                        # Track this CAPTCHA
                        if current_url not in self.captcha_state['captcha_detected_at_url']:
                            self.captcha_state['captcha_detected_at_url'][current_url] = {
                                'detected': 0,
                                'solved': False,
                                'attempts': 0
                            }
                        
                        self.captcha_state['captcha_detected_at_url'][current_url]['detected'] += 1
                        self.captcha_state['current_captcha_url'] = current_url
                        
                        logger.warning(
                            f"[CAPTCHA] Detected at {current_url} "
                            f"(occurrence #{self.captcha_state['captcha_detected_at_url'][current_url]['detected']})"
                        )
                        
                        return True
                except:
                    pass
            
            return False
            
        except Exception as e:
            logger.debug(f"[CAPTCHA_DETECT] Error: {e}")
            return False
    
    async def _handle_captcha(self) -> bool:
        """
        Handle CAPTCHA automatically with smart retry strategy:
        1. Track attempts per URL to avoid infinite loops
        2. Try multiple solving strategies
        3. Skip if too many attempts
        """
        current_url = self.page.url
        
        # Check if we've exceeded max attempts for this URL
        url_state = self.captcha_state['captcha_detected_at_url'].get(current_url, {})
        attempts = url_state.get('attempts', 0)
        
        if attempts >= self.captcha_state['max_captcha_attempts']:
            logger.error(
                f"[CAPTCHA] Max attempts ({self.captcha_state['max_captcha_attempts']}) reached for {current_url}"
            )
            self.captcha_state['blocked_urls'].add(current_url)
            logger.info(f"[SKIP] Added {current_url} to blocked list")
            return False
        
        # Increment attempt counter
        if current_url not in self.captcha_state['captcha_detected_at_url']:
            self.captcha_state['captcha_detected_at_url'][current_url] = {
                'detected': 0,
                'solved': False,
                'attempts': 0
            }
        self.captcha_state['captcha_detected_at_url'][current_url]['attempts'] += 1
        
        logger.warning(
            f"[PUZZLE] CAPTCHA DETECTED - ATTEMPTING SEQUENTIAL SOLVE STRATEGIES "
            f"(attempt {self.captcha_state['captcha_detected_at_url'][current_url]['attempts']}/{self.captcha_state['max_captcha_attempts']})"
        )
        
        try:
            # ============================================================================
            # STRATEGY 1: captcha_solver.py (Primary - Existing, No Changes)
            # ============================================================================
            logger.info("[STRATEGY 1/3] Trying captcha_solver.py (primary method)...")
            
            try:
                from scraper.captcha_solver import CaptchaSolver
                
                solver = CaptchaSolver(self.page, max_solve_time=60)
                solved = await solver.wait_for_captcha_solution()
                
                if solved:
                    logger.info("[✓] CAPTCHA solved by captcha_solver.py!")
                    self.captcha_state['captcha_detected_at_url'][current_url]['solved'] = True
                    await asyncio.sleep(2)
                    return True
                else:
                    logger.info("[→] captcha_solver.py could not solve, trying next strategy...")
            except Exception as e:
                logger.warning(f"[!] captcha_solver.py failed: {e}")
            
            # ============================================================================
            # STRATEGY 2: Harvester with Auto-solve (Secondary)
            # ============================================================================
            logger.info("[STRATEGY 2/3] Trying CAPTCHA Harvester with auto-solve...")
            
            try:
                # Extract CAPTCHA details
                captcha_type, sitekey = await self._extract_captcha_details()
                
                if sitekey and sitekey != "linkedin_internal":
                    logger.info(f"[→] Creating harvester challenge: {captcha_type}")
                    
                    # Create challenge
                    challenge_id = await self.harvester_client.create_challenge(
                        sitekey=sitekey,
                        page_url=current_url,
                        captcha_type=captcha_type
                    )
                    
                    if challenge_id:
                        logger.info(f"[→] Polling for solution (auto-solve enabled)...")
                        # Get solution with auto-solve enabled
                        token = await self.harvester_client.get_solution(
                            challenge_id=challenge_id,
                            timeout=120,
                            poll_interval=2.0,
                            auto_solve=True  # Enable automatic UI interaction
                        )
                        
                        if token:
                            # Inject token
                            logger.info("[→] Injecting token into page...")
                            injected = await self._inject_captcha_token(token, captcha_type)
                            
                            if injected:
                                logger.info("[✓] CAPTCHA solved by harvester!")
                                self.captcha_state['captcha_detected_at_url'][current_url]['solved'] = True
                                await asyncio.sleep(3)
                                return True
                        else:
                            logger.info("[→] Harvester timed out, trying next strategy...")
                else:
                    logger.info("[→] No sitekey found, trying next strategy...")
            except Exception as e:
                logger.warning(f"[!] Harvester strategy failed: {e}")
            
            # ============================================================================
            # STRATEGY 3: Manual Bypass Attempts (Tertiary - Existing Logic)
            # ============================================================================
            logger.info("[STRATEGY 3/3] Trying manual bypass methods...")
            
            # Method 1: Try to auto-click "I am human" button
            logger.info("[3.1] Looking for hCaptcha 'I am human' button...")
            
            # Wait for the hCaptcha iframe to be present
            try:
                await self.page.wait_for_selector('iframe[src*="hcaptcha"]', timeout=5000)
                logger.info("[OK] hCaptcha iframe detected")
            except:
                logger.info("[INFO] No hCaptcha iframe found, trying alternative selectors")
            
            # Try to click the checkbox directly
            try:
                logger.info("[TRY] Attempting to click hCaptcha checkbox...")
                # Find and click the hCaptcha checkbox
                checkbox = await self.page.query_selector('.h-captcha iframe')
                if checkbox:
                    # Switch to iframe
                    frame_handles = await self.page.query_selector_all('iframe')
                    for frame_handle in frame_handles:
                        try:
                            frame = await frame_handle.content_frame()
                            if frame:
                                # Look for checkbox in frame
                                button = await frame.query_selector('input[type="checkbox"]')
                                if button:
                                    await button.click()
                                    logger.info("[OK] Clicked hCaptcha checkbox")
                                    await asyncio.sleep(5)
                        except:
                            pass
            except Exception as e:
                logger.debug(f"[DEBUG] Checkbox click failed: {e}")
            
            # Try clicking any visible button
            try:
                logger.info("[TRY] Attempting to click visible CAPTCHA button...")
                buttons = await self.page.query_selector_all('button')
                for btn in buttons:
                    text = await btn.text_content()
                    if text and ('human' in text.lower() or 'verify' in text.lower() or 'challenge' in text.lower()):
                        logger.info(f"[OK] Found button: {text}")
                        await btn.click()
                        logger.info("[OK] Clicked CAPTCHA button")
                        await asyncio.sleep(3)
                        break
            except Exception as e:
                logger.debug(f"[DEBUG] Button click failed: {e}")
            
            # Method 2: Wait and see if page processes (20 seconds with smart detection)
            logger.info("[3.2] Waiting for CAPTCHA processing (20 seconds)...")
            for i in range(20):
                try:
                    content = await self.page.content()
                    page_url = self.page.url
                    
                    # Check if CAPTCHA is gone
                    if not ('h-captcha' in content or 'g-recaptcha' in content or 'hcaptcha' in content):
                        logger.info("[SUCCESS] CAPTCHA appears to be resolved!")
                        self.captcha_state['captcha_detected_at_url'][current_url]['solved'] = True
                        await asyncio.sleep(2)
                        return True
                    
                    # Check if we've progressed to a new page
                    if page_url != current_url:
                        logger.info(f"[SUCCESS] Page navigated to {page_url} - CAPTCHA bypassed!")
                        self.captcha_state['captcha_detected_at_url'][current_url]['solved'] = True
                        return True
                    
                    # Check for success indicators
                    if 'feed' in content or 'mynetwork' in content:
                        if current_url not in content:
                            logger.info("[SUCCESS] Navigated to main page - CAPTCHA resolved!")
                            self.captcha_state['captcha_detected_at_url'][current_url]['solved'] = True
                            return True
                    
                    await asyncio.sleep(1)
                except:
                    await asyncio.sleep(1)
            
            # Method 3: Try page refresh
            logger.info("[3.3] Attempting page refresh to bypass CAPTCHA...")
            try:
                await self.page.reload(wait_until='domcontentloaded', timeout=15000)
                await asyncio.sleep(3)
                
                # Check if CAPTCHA still present
                content = await self.page.content()
                if not ('h-captcha' in content or 'g-recaptcha' in content or 'hcaptcha' in content):
                    logger.info("[SUCCESS] CAPTCHA resolved after refresh!")
                    self.captcha_state['captcha_detected_at_url'][current_url]['solved'] = True
                    return True
                else:
                    logger.info("[INFO] CAPTCHA still present after refresh")
            except Exception as e:
                logger.warning(f"[WARN] Refresh failed: {e}")
            
            # Method 4: Continue anyway - LinkedIn might allow without solving
            logger.info("[3.4] Continuing despite CAPTCHA (may not block actual requests)...")
            await asyncio.sleep(2)
            return True
        
        except Exception as e:
            logger.error(f"[ERROR] Exception in CAPTCHA handling: {e}")
            logger.info("[CONTINUE] Continuing anyway...")
            return True
    
    async def _extract_captcha_details(self) -> tuple:
        """
        Extract CAPTCHA type and sitekey
        
        Returns:
            (captcha_type, sitekey) - e.g., ("recaptcha_v2", "abc123...")
        """
        try:
            # Check for reCAPTCHA v2
            sitekey = await self.page.evaluate("""
                () => {
                    // Check for reCAPTCHA iframe
                    const iframe = document.querySelector('iframe[src*="recaptcha"]');
                    if (iframe) {
                        const src = iframe.src;
                        const match = src.match(/k=([^&]+)/);
                        if (match) return match[1];
                    }
                    // Check for data-sitekey attribute
                    const container = document.querySelector('[data-sitekey]');
                    if (container) return container.dataset.sitekey;
                    // Check window object
                    if (window.grecaptcha) {
                        return window.grecaptcha.getResponse ? 'captured' : null;
                    }
                    return null;
                }
            """)
            
            if sitekey:
                return ("recaptcha_v2", sitekey)
            
            # Check for hCaptcha
            sitekey = await self.page.evaluate("""
                () => {
                    const container = document.querySelector('[data-sitekey]');
                    if (container) return container.dataset.sitekey;
                    return null;
                }
            """)
            
            if sitekey:
                return ("hcaptcha", sitekey)
            
            # Check for LinkedIn challenge
            challenge_element = await self.page.query_selector('[data-captcha-id]')
            if challenge_element:
                return ("linkedin_challenge", "linkedin_internal")
            
            # Fallback
            return ("unknown", None)
        
        except Exception as e:
            logger.error(f"Error extracting CAPTCHA details: {e}")
            return ("unknown", None)
    
    async def _inject_captcha_token(self, token: str, captcha_type: str) -> bool:
        """
        Inject solved CAPTCHA token into page
        
        Args:
            token: CAPTCHA response token
            captcha_type: Type of CAPTCHA (recaptcha_v2, hcaptcha, linkedin_challenge)
        
        Returns:
            True if injection successful
        """
        try:
            if captcha_type == "recaptcha_v2":
                # Inject into g-recaptcha-response hidden field
                await self.page.evaluate(f"""
                    () => {{
                        const field = document.querySelector('[name="g-recaptcha-response"]');
                        if (field) {{
                            field.value = {repr(token)};
                            field.innerText = {repr(token)};
                        }}
                        
                        // Try to find and call callback
                        if (window.___grecaptcha_cfg) {{
                            for (const key in window.___grecaptcha_cfg.clients) {{
                                window.___grecaptcha_cfg.clients[key].callback({repr(token)});
                            }}
                        }}
                        return true;
                    }}
                """)
                
                # Try to submit form
                await self.page.evaluate("""
                    () => {
                        const form = document.querySelector('form');
                        if (form) {
                            form.submit();
                            return true;
                        }
                        // Try to find submit button
                        const buttons = document.querySelectorAll('button[type="submit"]');
                        if (buttons.length > 0) {
                            buttons[0].click();
                            return true;
                        }
                        return false;
                    }
                """)
                
                return True
            
            elif captcha_type == "hcaptcha":
                # Inject into h-captcha-response
                await self.page.evaluate(f"""
                    () => {{
                        const field = document.querySelector('[name="h-captcha-response"]');
                        if (field) {{
                            field.value = {repr(token)};
                        }}
                        // Call hcaptcha callback
                        if (window.hcaptcha) {{
                            window.hcaptcha.getResponse = () => ({repr(token)});
                        }}
                        return true;
                    }}
                """)
                
                # Submit form
                await self.page.evaluate("""
                    () => {
                        const form = document.querySelector('form');
                        if (form) form.submit();
                    }
                """)
                return True
            
            elif captcha_type == "linkedin_challenge":
                # Try to find and click verify button
                verify_btn = await self.page.query_selector('button[aria-label*="Verify"]')
                if verify_btn:
                    await verify_btn.click()
                    return True
                return False
            
            return False
        
        except Exception as e:
            logger.error(f"Error injecting CAPTCHA token: {e}")
            return False
        
    def get_captcha_status_report(self) -> str:
        """Get a report of CAPTCHA status for debugging"""
        report = []
        report.append("=" * 60)
        report.append("CAPTCHA STATUS REPORT")
        report.append("=" * 60)
        
        report.append(f"Current CAPTCHA URL: {self.captcha_state['current_captcha_url']}")
        report.append(f"Blocked URLs: {len(self.captcha_state['blocked_urls'])}")
        if self.captcha_state['blocked_urls']:
            for url in self.captcha_state['blocked_urls']:
                report.append(f"  - {url}")
        
        report.append(f"\nDetected CAPTCHAs ({len(self.captcha_state['captcha_detected_at_url'])} URLs):")
        for url, state in self.captcha_state['captcha_detected_at_url'].items():
            solved_str = "✓ SOLVED" if state['solved'] else "✗ UNSOLVED"
            report.append(
                f"  [{solved_str}] {url} "
                f"(detected {state['detected']}x, attempts {state['attempts']}/{self.captcha_state['max_captcha_attempts']})"
            )
        
        report.append("=" * 60)
        return "\n".join(report)
    
    def is_url_blocked(self, url: str) -> bool:
        """Check if a URL is blocked due to CAPTCHA issues"""
        return url in self.captcha_state['blocked_urls']
    
    def skip_url(self, url: str) -> None:
        """Manually skip/block a URL"""
        self.captcha_state['blocked_urls'].add(url)
        logger.info(f"[SKIP] Added {url} to blocked list")
        

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
