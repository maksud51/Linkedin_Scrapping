"""
CAPTCHA Harvester Client Integration
====================================
This module handles communication between the scraper and the CAPTCHA harvester service.
When a CAPTCHA is detected, it creates a challenge in the harvester and polls for the solution.

Usage:
    client = HarvesterClient("http://localhost:8000")
    challenge_id = await client.create_challenge(sitekey, page_url, "recaptcha_v2")
    token = await client.get_solution(challenge_id, timeout=300)
"""

import asyncio
import aiohttp
import logging
from typing import Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class HarvesterClient:
    """Client for communicating with CAPTCHA harvester service"""
    
    def __init__(self, harvester_url: str = "http://localhost:8000", timeout: int = 10):
        """
        Initialize harvester client
        
        Args:
            harvester_url: Base URL of harvester service (e.g., http://localhost:8000)
            timeout: Request timeout in seconds
        """
        self.harvester_url = harvester_url.rstrip('/')
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(timeout=self.timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=self.timeout)
        return self.session
    
    async def health_check(self) -> bool:
        """Check if harvester service is running"""
        try:
            session = await self._get_session()
            async with session.get(f"{self.harvester_url}/api/stats") as response:
                return response.status == 200
        except Exception as e:
            logger.error(f"Harvester health check failed: {e}")
            return False
    
    async def create_challenge(
        self,
        sitekey: str,
        page_url: str,
        captcha_type: str = "recaptcha_v2"
    ) -> Optional[str]:
        """
        Create a new CAPTCHA challenge in the harvester
        
        Args:
            sitekey: reCAPTCHA/hCaptcha sitekey
            page_url: URL where CAPTCHA appeared
            captcha_type: Type of CAPTCHA (recaptcha_v2, hcaptcha, linkedin_challenge)
        
        Returns:
            Challenge ID if successful, None otherwise
        """
        try:
            session = await self._get_session()
            
            params = {
                "sitekey": sitekey,
                "page_url": page_url,
                "captcha_type": captcha_type
            }
            
            async with session.post(
                f"{self.harvester_url}/api/challenge/create",
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    challenge_id = data.get("challenge_id")
                    logger.info(f"[+] Challenge created: {challenge_id} ({captcha_type})")
                    return challenge_id
                else:
                    logger.error(f"Failed to create challenge: {response.status}")
                    return None
        
        except Exception as e:
            logger.error(f"Error creating challenge: {e}")
            return None
    
    async def get_solution(
        self,
        challenge_id: str,
        timeout: int = 300,
        poll_interval: float = 2.0,
        auto_solve: bool = True
    ) -> Optional[str]:
        """
        Poll for CAPTCHA solution with automatic UI interaction fallback
        
        Args:
            challenge_id: Challenge ID from create_challenge()
            timeout: Max seconds to wait for solution
            poll_interval: Seconds between polls
            auto_solve: Enable automatic UI interaction if manual solving times out
        
        Returns:
            Solved CAPTCHA token if successful, None if timeout
        """
        start_time = asyncio.get_event_loop().time()
        deadline = start_time + timeout
        auto_solve_attempted = False
        
        logger.info(f"[→] Polling for solution: {challenge_id} (timeout: {timeout}s)")
        
        while True:
            try:
                session = await self._get_session()
                
                async with session.get(
                    f"{self.harvester_url}/api/challenge/{challenge_id}/solution"
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if data.get("token"):
                            logger.info(f"[✓] Solution received: {challenge_id[:8]}...")
                            return data["token"]
                        
                        # Still pending
                        elapsed = asyncio.get_event_loop().time() - start_time
                        remaining = timeout - elapsed
                        
                        # Try automatic solving after half the timeout has passed
                        if auto_solve and not auto_solve_attempted and elapsed > (timeout * 0.5):
                            logger.info(f"[🤖] Attempting automatic CAPTCHA solving...")
                            auto_solve_attempted = True
                            solved = await self._auto_solve_captcha(challenge_id)
                            if solved:
                                logger.info(f"[✓] Auto-solve succeeded!")
                                return solved
                        
                        logger.debug(f"[⏳] Waiting for solution... ({remaining:.0f}s remaining)")
                    else:
                        logger.warning(f"API error: {response.status}")
            
            except Exception as e:
                logger.error(f"Error polling solution: {e}")
            
            # Check timeout
            if asyncio.get_event_loop().time() >= deadline:
                logger.error(f"[✗] Solution timeout: {challenge_id}")
                return None
            
            # Wait before next poll
            await asyncio.sleep(poll_interval)
    
    async def _auto_solve_captcha(self, challenge_id: str) -> Optional[str]:
        """
        Automatic CAPTCHA solving using UI automation
        Tries multiple strategies to solve CAPTCHA automatically
        
        Args:
            challenge_id: Challenge ID to solve
        
        Returns:
            Solved token if successful, None otherwise
        """
        try:
            from playwright.async_api import async_playwright
            
            logger.info(f"[🤖] Starting automatic CAPTCHA solver for {challenge_id}")
            
            async with async_playwright() as p:
                # Launch browser with specific viewport
                browser = await p.chromium.launch(headless=False)
                context = await browser.new_context(
                    viewport={'width': 1280, 'height': 720},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                )
                page = await context.new_page()
                
                try:
                    # Navigate to harvester UI
                    harvester_ui_url = f"{self.harvester_url}/challenge/{challenge_id}"
                    logger.info(f"[→] Opening harvester UI: {harvester_ui_url}")
                    await page.goto(harvester_ui_url, wait_until='networkidle', timeout=10000)
                    
                    # Wait for CAPTCHA frame to load
                    await asyncio.sleep(2)
                    
                    # Strategy 1: Try to find and click "I am human" checkbox
                    logger.info("[1/4] Looking for 'I am human' checkbox...")
                    checkbox_found = await self._try_click_human_checkbox(page)
                    
                    if checkbox_found:
                        await asyncio.sleep(3)  # Wait for potential token
                        token = await self._extract_token_from_page(page)
                        if token:
                            logger.info("[✓] Token extracted after checkbox click!")
                            await browser.close()
                            return token
                    
                    # Strategy 2: Try to interact with reCAPTCHA iframe
                    logger.info("[2/4] Attempting reCAPTCHA iframe interaction...")
                    iframe_solved = await self._try_recaptcha_iframe(page)
                    
                    if iframe_solved:
                        await asyncio.sleep(3)
                        token = await self._extract_token_from_page(page)
                        if token:
                            logger.info("[✓] Token extracted after iframe interaction!")
                            await browser.close()
                            return token
                    
                    # Strategy 3: Try hCaptcha interaction
                    logger.info("[3/4] Attempting hCaptcha interaction...")
                    hcaptcha_solved = await self._try_hcaptcha_interaction(page)
                    
                    if hcaptcha_solved:
                        await asyncio.sleep(3)
                        token = await self._extract_token_from_page(page)
                        if token:
                            logger.info("[✓] Token extracted after hCaptcha!")
                            await browser.close()
                            return token
                    
                    # Strategy 4: Check if token already exists (maybe manually solved)
                    logger.info("[4/4] Checking for existing token...")
                    for _ in range(10):  # Poll for 20 seconds
                        token = await self._extract_token_from_page(page)
                        if token:
                            logger.info("[✓] Token found (possibly manually solved)!")
                            await browser.close()
                            return token
                        await asyncio.sleep(2)
                    
                    logger.warning("[!] All auto-solve strategies failed")
                    await browser.close()
                    return None
                
                except Exception as e:
                    logger.error(f"Error in auto-solve: {e}")
                    await browser.close()
                    return None
        
        except Exception as e:
            logger.error(f"Failed to initialize auto-solver: {e}")
            return None
    
    async def _try_click_human_checkbox(self, page) -> bool:
        """Try to find and click 'I am human' or similar checkbox"""
        try:
            # Try different selectors for checkbox
            selectors = [
                'iframe[src*="recaptcha"]',
                'iframe[src*="hcaptcha"]',
                '[role="checkbox"]',
                '.recaptcha-checkbox',
                '#recaptcha-anchor'
            ]
            
            for selector in selectors:
                try:
                    # Check if iframe exists
                    if 'iframe' in selector:
                        frames = page.frames
                        for frame in frames:
                            if 'recaptcha' in frame.url or 'hcaptcha' in frame.url:
                                logger.info(f"[→] Found CAPTCHA iframe: {frame.url}")
                                # Try to click checkbox within iframe
                                try:
                                    checkbox = await frame.query_selector('.recaptcha-checkbox-border, #checkbox, [role="checkbox"]')
                                    if checkbox:
                                        await checkbox.click()
                                        logger.info("[✓] Clicked checkbox in iframe")
                                        return True
                                except:
                                    pass
                    else:
                        # Try direct selector
                        element = await page.query_selector(selector)
                        if element:
                            await element.click()
                            logger.info(f"[✓] Clicked element: {selector}")
                            return True
                except:
                    continue
            
            return False
        except Exception as e:
            logger.debug(f"Checkbox click failed: {e}")
            return False
    
    async def _try_recaptcha_iframe(self, page) -> bool:
        """Try to interact with reCAPTCHA iframe"""
        try:
            frames = page.frames
            for frame in frames:
                if 'recaptcha' in frame.url.lower():
                    # Try to click the checkbox
                    try:
                        await frame.click('.recaptcha-checkbox-border', timeout=2000)
                        logger.info("[✓] Clicked reCAPTCHA checkbox")
                        await asyncio.sleep(2)
                        
                        # Check if challenge appeared
                        challenge_frame = None
                        for f in page.frames:
                            if 'recaptcha' in f.url and 'bframe' in f.url:
                                challenge_frame = f
                                break
                        
                        if challenge_frame:
                            logger.info("[→] Challenge frame detected")
                            # Wait for manual solving or auto-detection
                            await asyncio.sleep(5)
                        
                        return True
                    except:
                        pass
            return False
        except Exception as e:
            logger.debug(f"reCAPTCHA iframe interaction failed: {e}")
            return False
    
    async def _try_hcaptcha_interaction(self, page) -> bool:
        """Try to interact with hCaptcha"""
        try:
            frames = page.frames
            for frame in frames:
                if 'hcaptcha' in frame.url.lower():
                    try:
                        await frame.click('#checkbox', timeout=2000)
                        logger.info("[✓] Clicked hCaptcha checkbox")
                        await asyncio.sleep(3)
                        return True
                    except:
                        pass
            return False
        except Exception as e:
            logger.debug(f"hCaptcha interaction failed: {e}")
            return False
    
    async def _extract_token_from_page(self, page) -> Optional[str]:
        """Extract CAPTCHA token from page"""
        try:
            # Try to get token from various sources
            token = await page.evaluate("""
                () => {
                    // Check g-recaptcha-response
                    const gResponse = document.querySelector('[name="g-recaptcha-response"]');
                    if (gResponse && gResponse.value) {
                        return gResponse.value;
                    }
                    
                    // Check h-captcha-response
                    const hResponse = document.querySelector('[name="h-captcha-response"]');
                    if (hResponse && hResponse.value) {
                        return hResponse.value;
                    }
                    
                    // Check window object
                    if (window.captchaToken) {
                        return window.captchaToken;
                    }
                    
                    return null;
                }
            """)
            
            return token if token else None
        except Exception as e:
            logger.debug(f"Token extraction failed: {e}")
            return None
    
    async def get_stats(self) -> dict:
        """Get harvester statistics"""
        try:
            session = await self._get_session()
            async with session.get(f"{self.harvester_url}/api/stats") as response:
                if response.status == 200:
                    return await response.json()
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
        return {}
    
    async def close(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()


# Convenience functions for standalone usage
_default_client: Optional[HarvesterClient] = None


async def initialize_harvester(harvester_url: str = "http://localhost:8000"):
    """Initialize global harvester client"""
    global _default_client
    _default_client = HarvesterClient(harvester_url)
    
    # Check if service is running
    if not await _default_client.health_check():
        logger.warning(
            f"Harvester service not responding at {harvester_url}\n"
            f"Make sure to run: python captcha_harvester.py"
        )
        return False
    
    logger.info(f"[✓] Harvester service connected: {harvester_url}")
    return True


async def create_captcha_challenge(
    sitekey: str,
    page_url: str,
    captcha_type: str = "recaptcha_v2"
) -> Optional[str]:
    """Create CAPTCHA challenge using default client"""
    if _default_client is None:
        raise RuntimeError("Harvester not initialized. Call initialize_harvester() first")
    return await _default_client.create_challenge(sitekey, page_url, captcha_type)


async def get_captcha_solution(
    challenge_id: str,
    timeout: int = 300
) -> Optional[str]:
    """Get CAPTCHA solution using default client"""
    if _default_client is None:
        raise RuntimeError("Harvester not initialized. Call initialize_harvester() first")
    return await _default_client.get_solution(challenge_id, timeout)


async def close_harvester():
    """Close default harvester client"""
    if _default_client:
        await _default_client.close()
