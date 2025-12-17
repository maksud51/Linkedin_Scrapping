"""
Automatic CAPTCHA Solver with Advanced Techniques
Handles various CAPTCHA types with multiple solving strategies
"""

import asyncio
import logging
import random
from typing import Optional, Tuple
from playwright.async_api import Page

logger = logging.getLogger(__name__)


class CaptchaSolver:
    """Automatic CAPTCHA solving with advanced techniques and fallbacks"""
    
    def __init__(self, page: Page):
        self.page = page
        self.solving_attempts = 0
        self.max_attempts = 5  # Increased from 3
        self.last_solved_url = None
        self.solved_urls_cache = set()  # Cache of successfully solved URLs
    
    async def is_captcha_already_solved_on_page(self) -> bool:
        """Check if CAPTCHA on current page was already solved"""
        try:
            current_url = self.page.url
            if current_url in self.solved_urls_cache:
                logger.info(f"[CACHE] CAPTCHA on {current_url} already solved in this session")
                return True
            return False
        except:
            return False
    
    async def mark_captcha_as_solved(self) -> None:
        """Mark current page's CAPTCHA as solved"""
        try:
            current_url = self.page.url                    
            self.solved_urls_cache.add(current_url)
            self.last_solved_url = current_url
            logger.info(f"[CACHE] Marked {current_url} as solved")
        except:
            pass
    
    async def solve_recaptcha_v2(self) -> bool:
        """Solve reCAPTCHA v2 with advanced aggressive techniques"""
        try:
            logger.info("[reCAPTCHA] Attempting aggressive reCAPTCHA v2 solver...")
            
            # Technique 1: Execute grecaptcha.execute() and wait for callback
            logger.debug("[reCAPTCHA] T1: grecaptcha.execute() trigger...")
            try:
                result = await self.page.evaluate("""
                    async () => {
                        // Try multiple execute methods
                        const promises = [];
                        
                        // Method 1: Direct execute
                        if (window.grecaptcha && window.grecaptcha.execute) {
                            promises.push((async () => {
                                try {
                                    const token = await window.grecaptcha.execute();
                                    if (token && token.length > 100) return token;
                                } catch(e) {}
                                return null;
                            })());
                        }
                        
                        // Method 2: Try to trigger via reset and execute
                        if (window.grecaptcha && window.grecaptcha.reset) {
                            promises.push((async () => {
                                try {
                                    window.grecaptcha.reset();
                                    const token = await window.grecaptcha.execute();
                                    if (token && token.length > 100) return token;
                                } catch(e) {}
                                return null;
                            })());
                        }
                        
                        // Wait for any result
                        for (let p of promises) {
                            try {
                                const token = await p;
                                if (token) return token;
                            } catch(e) {}
                        }
                        
                        // Check response field
                        const responseElem = document.querySelector('[name="g-recaptcha-response"]');
                        if (responseElem && responseElem.value && responseElem.value.length > 100) {
                            return responseElem.value;
                        }
                        
                        return null;
                    }
                """, timeout=15000)
                
                if result:
                    logger.info(f"[✓] Token obtained via execute: {len(result) if result else 0} chars")
                    if result and len(result) > 100:
                        await self._auto_submit_form()
                        return True
            except Exception as e:
                logger.debug(f"Execute failed: {e}")
            
            # Technique 2: Monitor response field while waiting
            logger.debug("[reCAPTCHA] T2: Response field monitoring...")
            for attempt in range(40):
                try:
                    token = await self.page.evaluate("""
                        () => {
                            const elem = document.querySelector('[name="g-recaptcha-response"]');
                            return (elem && elem.value && elem.value.length > 100) ? elem.value : null;
                        }
                    """)
                    
                    if token:
                        logger.info(f"[✓] Token from monitoring: {len(token)} chars")
                        await self._auto_submit_form()
                        return True
                except:
                    pass
                
                await asyncio.sleep(0.5)
            
            # Technique 3: Click the checkbox multiple times
            logger.debug("[reCAPTCHA] T3: Checkbox click strategy...")
            try:
                # Find and click checkbox
                selectors = [
                    'div.g-recaptcha',
                    'div.recaptcha-checkbox-border',
                    '#recaptcha-anchor',
                    '[aria-label*="robot"]'
                ]
                
                for selector in selectors:
                    try:
                        elem = await self.page.query_selector(selector)
                        if elem:
                            await elem.click(timeout=5000)
                            logger.debug(f"Clicked {selector}")
                            await asyncio.sleep(2)
                            
                            token = await self.page.evaluate("""
                                () => {
                                    const elem = document.querySelector('[name="g-recaptcha-response"]');
                                    return (elem && elem.value && elem.value.length > 100) ? elem.value : null;
                                }
                            """)
                            
                            if token:
                                logger.info(f"[✓] Token after click: {len(token)} chars")
                                await self._auto_submit_form()
                                return True
                    except:
                        pass
            except Exception as e:
                logger.debug(f"Click strategy failed: {e}")
            
            # Technique 4: Wait longer with page interaction
            logger.debug("[reCAPTCHA] T4: Extended wait with interaction...")
            for attempt in range(30):
                try:
                    # Try to interact with page
                    await self.page.mouse.move(500, 300)
                    await asyncio.sleep(0.2)
                    
                    token = await self.page.evaluate("""
                        () => {
                            const elem = document.querySelector('[name="g-recaptcha-response"]');
                            return (elem && elem.value && elem.value.length > 100) ? elem.value : null;
                        }
                    """)
                    
                    if token:
                        logger.info(f"[✓] Token after interaction: {len(token)} chars")
                        await self._auto_submit_form()
                        return True
                except:
                    pass
                
                await asyncio.sleep(0.5)
            
            logger.warning("[X] reCAPTCHA v2 solving failed - no token obtained")
            return False
            
        except Exception as e:
            logger.error(f"[X] reCAPTCHA v2 error: {e}")
            return False
    
    async def solve_hcaptcha(self) -> bool:
        """Solve hCaptcha with advanced techniques"""
        try:
            logger.info("[SOLVER] Solving hCaptcha...")
            
            await asyncio.sleep(1)
            
            # Method 1: Try JavaScript API
            try:
                logger.info("[INFO] Trying hCaptcha JavaScript API...")
                
                # Inject hCaptcha solver script
                await self.page.evaluate("""
                    () => {
                        if (window.hcaptcha) {
                            try {
                                // Get response
                                const response = window.hcaptcha.getResponse();
                                if (response && response.response) {
                                    // Set response in hidden field
                                    const field = document.querySelector('[name="h-captcha-response"]');
                                    if (field) {
                                        field.value = response.response;
                                        return response.response;
                                    }
                                }
                                return null;
                            } catch(e) {
                                return null;
                            }
                        }
                        return null;
                    }
                """)
                
                await asyncio.sleep(1)
                
                # Check if response is set
                response = await self.page.evaluate("""
                    () => document.querySelector('[name="h-captcha-response"]')?.value || null
                """)
                
                if response and len(response) > 50:
                    logger.info("[✓] hCaptcha solved via API!")
                    await self._auto_submit_form()
                    return True
            except:
                pass
            
            # Method 2: Monitor for response
            logger.info("[INFO] Waiting for hCaptcha response...")
            for attempt in range(30):
                try:
                    response = await self.page.evaluate("""
                        () => {
                            if (window.hcaptcha) {
                                const resp = window.hcaptcha.getResponse();
                                return resp ? resp.response || resp : null;
                            }
                            return null;
                        }
                    """)
                    
                    if response and len(str(response)) > 50:
                        logger.info("[✓] hCaptcha response obtained!")
                        await self._auto_submit_form()
                        return True
                except:
                    pass
                
                await asyncio.sleep(0.5)
            
            logger.warning("[WARN] hCaptcha - attempting other methods")
            return False
            
        except Exception as e:
            logger.debug(f"hCaptcha solve error: {e}")
            return False
    
    async def solve_linkedin_challenge(self) -> bool:
        """Solve LinkedIn challenge flows with advanced detection"""
        try:
            logger.info("[SOLVER] Attempting LinkedIn challenge solve...")
            
            # Check for checkpoint/verification
            try:
                # Method 1: Check for email verification
                email_input = await self.page.query_selector('input[type="email"]')
                if email_input:
                    logger.info("[INFO] Email verification required")
                    # Auto-fill with account email if possible
                    return False  # Need manual email
                
                # Method 2: Check for SMS code
                code_input = await self.page.query_selector('input[placeholder*="code" i]')
                if code_input:
                    logger.info("[INFO] Code verification required")
                    return False  # Need manual code
                
                # Method 3: Look for automatic challenge resolution
                challenge_form = await self.page.query_selector('form')
                if challenge_form:
                    # Try to detect and click verify button
                    buttons = await self.page.query_selector_all('button')
                    for button in buttons:
                        text = await button.text_content()
                        if text and ('verify' in text.lower() or 'continue' in text.lower() or 'confirm' in text.lower()):
                            await button.click()
                            await asyncio.sleep(2)
                            logger.info("[✓] Challenge button clicked")
                            return True
                
            except:
                pass
            
            logger.warning("[WARN] Could not solve LinkedIn challenge")
            return False
            
        except Exception as e:
            logger.debug(f"LinkedIn challenge solve error: {e}")
            return False
    
    async def solve_with_stealth_bypass(self) -> bool:
        """Advanced stealth bypass to prevent CAPTCHA altogether"""
        try:
            logger.info("[STEALTH] Applying advanced stealth techniques...")
            
            await self.page.evaluate("""
                () => {
                    // Remove all automation detection methods
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => false,
                    });
                    
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5],
                    });
                    
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['en-US', 'en'],
                    });
                    
                    window.chrome = {
                        runtime: {},
                    };
                    
                    // Spoof permissions
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                            Promise.resolve({ state: Notification.permission }) :
                            originalQuery(parameters)
                    );
                    
                    // Remove headless detection
                    Object.defineProperty(navigator, 'headless', {
                        get: () => false,
                    });
                    
                    // Spoof screen
                    Object.defineProperty(screen, 'availHeight', {
                        get: () => 1080,
                    });
                    
                    Object.defineProperty(screen, 'availWidth', {
                        get: () => 1920,
                    });
                }
            """)
            
            logger.info("[✓] Stealth mode activated")
            return True
            
        except Exception as e:
            logger.debug(f"Stealth bypass error: {e}")
            return False
    
    async def _submit_captcha_form(self) -> bool:
        """Try to auto-submit form after CAPTCHA solve"""
        try:
            forms = await self.page.query_selector_all('form')
            for form in forms:
                # Find submit button
                submit_btn = await form.query_selector('button[type="submit"]')
                if not submit_btn:
                    submit_btn = await form.query_selector('button')
                
                if submit_btn:
                    await submit_btn.click()
                    await asyncio.sleep(2)
                    logger.info("[✓] Form submitted")
                    return True
            return False
        except:
            return False
    
    async def _auto_submit_form(self) -> bool:
        """Auto-submit form after CAPTCHA solution"""
        try:
            # Method 1: Find and click submit button
            submit_buttons = await self.page.query_selector_all(
                'button[type="submit"], button[aria-label*="continue"], button:has-text("Continue"), button:has-text("Submit")'
            )
            
            for btn in submit_buttons:
                try:
                    await btn.click(timeout=5000)
                    await asyncio.sleep(2)
                    logger.info("[✓] Form auto-submitted")
                    return True
                except:
                    continue
            
            # Method 2: Try pressing Enter
            try:
                await self.page.press('body', 'Enter')
                await asyncio.sleep(2)
                return True
            except:
                pass
            
            return False
        except Exception as e:
            logger.debug(f"Auto-submit error: {e}")
            return False
    
    async def _is_recaptcha_solved(self) -> bool:
        """Check if reCAPTCHA is solved"""
        try:
            result = await self.page.evaluate("""
                () => {
                    const response = document.querySelector('[name="g-recaptcha-response"]');
                    return response && response.value ? true : false;
                }
            """)
            return result
        except:
            return False
    
    async def detect_captcha_type(self) -> Optional[str]:
        """Detect CAPTCHA type from page content"""
        try:
            content = await self.page.content()
            
            if 'recaptcha' in content.lower():
                return 'recaptcha'
            elif 'hcaptcha' in content.lower():
                return 'hcaptcha'
            elif 'checkpoint' in content.lower() or 'verify' in content.lower():
                return 'linkedin_checkpoint'
            
            return None
            
        except:
            return None
    
    async def solve_automatically(self, timeout: int = 600000) -> bool:
        """Comprehensive automatic solving with all strategies"""
        self.solving_attempts += 1
        
        if self.solving_attempts > self.max_attempts:
            logger.warning(f"[X] Max solve attempts ({self.max_attempts}) reached")
            return False
        
        logger.info(f"\n[SOLVER] Auto-solve attempt {self.solving_attempts}/{self.max_attempts}")
        
        try:
            # Apply stealth first
            await self.solve_with_stealth_bypass()
            await asyncio.sleep(2)
            
            # Detect type
            captcha_type = await self.detect_captcha_type()
            logger.info(f"[DETECT] CAPTCHA type: {captcha_type}")
            
            # Try solving based on type
            if captcha_type == 'recaptcha':
                logger.info("[ATTEMPT] Trying reCAPTCHA solver...")
                if await self.solve_recaptcha_v2():
                    return True
            
            if captcha_type == 'hcaptcha':
                logger.info("[ATTEMPT] Trying hCaptcha solver...")
                if await self.solve_hcaptcha():
                    return True
            
            if captcha_type == 'linkedin_checkpoint':
                logger.info("[ATTEMPT] Trying LinkedIn challenge solver...")
                if await self.solve_linkedin_challenge():
                    return True
            
            # Try all methods regardless of type
            logger.info("[ATTEMPT] Trying all solver methods...")
            
            if await self.solve_recaptcha_v2():
                return True
            
            if await self.solve_hcaptcha():
                return True
            
            if await self.solve_linkedin_challenge():
                return True
            
            # Monitor for page changes (15 seconds)
            logger.info("[MONITOR] Watching for page changes...")
            start_url = self.page.url
            
            for monitor_attempt in range(15):
                await asyncio.sleep(1)
                
                # Check if navigated
                if self.page.url != start_url:
                    logger.info("[✓] Page navigated - CAPTCHA solved!")
                    return True
                
                # Check if CAPTCHA gone
                try:
                    content = await self.page.content()
                    if ('captcha' not in content.lower() and 
                        'recaptcha' not in content.lower() and
                        'hcaptcha' not in content.lower()):
                        logger.info("[✓] CAPTCHA indicators removed!")
                        return True
                except:
                    pass
            
            logger.warning("[X] Automatic solving failed")
            return False
            
        except Exception as e:
            logger.error(f"[X] Solver error: {e}")
            return False
    
    async def wait_for_captcha_solution(self, timeout: int = 600000) -> bool:
        """Wait for CAPTCHA solution with continuous monitoring and retry"""
        logger.info(f"[CAPTCHA] Starting automatic solution (timeout: {timeout//1000}s)")
        
        # Check if already solved on this page
        if await self.is_captcha_already_solved_on_page():
            return True
        
        start_time = asyncio.get_event_loop().time()
        
        while True:
            elapsed = (asyncio.get_event_loop().time() - start_time) * 1000
            remaining = timeout - elapsed
            
            if elapsed > timeout:
                logger.error(f"[X] CAPTCHA timeout after {timeout//1000}s")
                return False
            
            logger.info(f"[TRY] Solving attempt... ({remaining//1000}s remaining)")
            
            # Try automatic solve
            if await self.solve_automatically(int(remaining)):
                logger.info("[SUCCESS] CAPTCHA solved!")
                await self.mark_captcha_as_solved()
                await asyncio.sleep(2)
                return True
            
            # If still here, monitor for manual solve
            logger.info("[MONITOR] Waiting for manual solution or page change...")
            
            try:
                start_url = self.page.url
                
                # Monitor for 10 seconds
                for i in range(10):
                    await asyncio.sleep(1)
                    
                    # Check page navigation
                    try:
                        if self.page.url != start_url:
                            logger.info("[✓] Page changed - continuing")
                            await self.mark_captcha_as_solved()
                            return True
                    except:
                        pass
                    
                    # Check CAPTCHA gone
                    try:
                        content = await self.page.content()
                        if ('captcha' not in content.lower() and 
                            'recaptcha' not in content.lower() and
                            'hcaptcha' not in content.lower() and
                            'verify' not in content.lower()):
                            logger.info("[✓] CAPTCHA indicators gone")
                            await self.mark_captcha_as_solved()
                            return True
                    except:
                        pass
                
            except Exception as e:
                logger.debug(f"Monitor error: {e}")
            
            # Check timeout again before retry
            elapsed = (asyncio.get_event_loop().time() - start_time) * 1000
            if elapsed > timeout:
                logger.error(f"[X] CAPTCHA timeout")
                return False
            
            logger.info(f"[RETRY] Retrying... ({(timeout-elapsed)//1000}s remaining)")

