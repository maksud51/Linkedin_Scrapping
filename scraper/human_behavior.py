"""
Human-Like Behavior Simulation
- Random delays and typing speed
- Gradual scrolling patterns
- Mouse movements
- Natural interaction patterns
"""

import asyncio
import random
from typing import Tuple
from playwright.async_api import Page
import logging

logger = logging.getLogger(__name__)


class HumanBehavior:
    """Simulate human-like browser behavior"""
    
    @staticmethod
    async def random_delay(min_seconds: float = 0.5, max_seconds: float = 3.0):
        """Random delay between actions (human-like) with occasional longer pauses"""
        # Occasionally add longer pauses (every 1 in 5 times)
        if random.random() < 0.2:
            delay = random.uniform(max_seconds * 1.5, max_seconds * 2.5)
        else:
            delay = random.uniform(min_seconds, max_seconds)
        await asyncio.sleep(delay)
    
    @staticmethod
    async def human_type(page: Page, selector: str, text: str, delay_min: float = 0.05, delay_max: float = 0.15):
        """Type text with human-like speed variation"""
        try:
            await page.click(selector)
            await asyncio.sleep(random.uniform(0.1, 0.3))
            
            # Type with variable speed
            for char in text:
                await page.type(selector, char)
                await asyncio.sleep(random.uniform(delay_min, delay_max))
            
            logger.debug(f"Typed text in {selector}")
        except Exception as e:
            logger.error(f"Error typing text: {e}")
    
    @staticmethod
    async def human_scroll(page: Page, scroll_pattern: str = 'natural') -> bool:
        """Scroll page gradually like a human - multiple passes for full content loading"""
        try:
            # Scroll multiple times to ensure all dynamic content loads
            scroll_passes = 2
            
            for pass_num in range(scroll_passes):
                total_height = await page.evaluate('document.body.scrollHeight')
                viewport_height = await page.evaluate('window.innerHeight')
                
                if total_height <= viewport_height:
                    logger.debug(f"Page is fully visible (pass {pass_num + 1})")
                    if pass_num == 0:
                        continue
                    return True
                
                current_position = 0
                
                while current_position < total_height:
                    # Variable scroll distance (natural behavior)
                    if scroll_pattern == 'natural':
                        scroll_amount = random.randint(300, 800)
                    elif scroll_pattern == 'fast':
                        scroll_amount = random.randint(800, 1200)
                    else:  # slow
                        scroll_amount = random.randint(150, 400)
                    
                    current_position += scroll_amount
                    
                    # Scroll to position
                    await page.evaluate(f'window.scrollTo(0, {current_position})')
                    
                    # Random pause while scrolling (human reads content)
                    await asyncio.sleep(random.uniform(0.4, 1.2))
                    
                    # Sometimes scroll back up (human re-reading)
                    if random.random() < 0.08:
                        back_scroll = random.randint(100, 250)
                        current_position -= back_scroll
                        await page.evaluate(f'window.scrollTo(0, {current_position})')
                        await asyncio.sleep(random.uniform(0.4, 1.0))
                    
                    # Periodic longer pause (human thinking)
                    if random.random() < 0.15:
                        await asyncio.sleep(random.uniform(2.0, 4.0))
                
                # Scroll back to top between passes
                if pass_num == 0:
                    await page.evaluate('window.scrollTo(0, 0)')
                    await asyncio.sleep(random.uniform(1, 2))
            
            logger.debug("Human-like scrolling completed (multiple passes)")
            return True
            
        except Exception as e:
            logger.error(f"Error during scroll: {e}")
            return False
    
    @staticmethod
    async def random_mouse_movement(page: Page, movement_count: int = None):
        """Random mouse movements like human"""
        try:
            if movement_count is None:
                movement_count = random.randint(3, 8)
            
            viewport = await page.evaluate('() => ({width: window.innerWidth, height: window.innerHeight})')
            
            for _ in range(movement_count):
                x = random.randint(100, viewport['width'] - 100)
                y = random.randint(100, viewport['height'] - 100)
                
                await page.mouse.move(x, y)
                await asyncio.sleep(random.uniform(0.1, 0.3))
            
            logger.debug("Random mouse movements completed")
            
        except Exception as e:
            logger.error(f"Error in mouse movement: {e}")
    
    @staticmethod
    async def human_click(page: Page, selector: str, delay_before: Tuple[float, float] = (0.5, 1.5),
                         delay_after: Tuple[float, float] = (0.5, 2.0)) -> bool:
        """Click with human-like timing"""
        try:
            # Delay before click
            await asyncio.sleep(random.uniform(delay_before[0], delay_before[1]))
            
            # Move mouse near element first
            element = await page.query_selector(selector)
            if element:
                await element.scroll_into_view_if_needed()
                await asyncio.sleep(random.uniform(0.1, 0.3))
            
            # Click
            await page.click(selector)
            
            # Delay after click
            await asyncio.sleep(random.uniform(delay_after[0], delay_after[1]))
            
            logger.debug(f"✅ Human-like click on {selector}")
            return True
            
        except Exception as e:
            logger.error(f"Error in human click: {e}")
            return False
    
    @staticmethod
    async def random_actions(page: Page):
        """Perform random actions to appear human"""
        try:
            actions = [
                lambda: HumanBehavior.random_mouse_movement(page, random.randint(1, 3)),
                lambda: HumanBehavior.random_delay(2, 5),
                lambda: page.evaluate('() => window.scrollBy(0, 100)'),
            ]
            
            random_action = random.choice(actions)
            if asyncio.iscoroutine(random_action):
                await random_action
            else:
                await random_action()
            
        except Exception as e:
            logger.debug(f"Random action error: {e}")
    
    @staticmethod
    async def wait_for_element_with_delay(page: Page, selector: str, timeout: int = 10000) -> bool:
        """Wait for element with random actions in between"""
        try:
            # Perform some random actions while waiting
            start_time = asyncio.get_event_loop().time()
            
            while asyncio.get_event_loop().time() - start_time < timeout / 1000:
                element = await page.query_selector(selector)
                if element:
                    logger.debug(f"✅ Element found: {selector}")
                    return True
                
                # Random actions while waiting
                await HumanBehavior.random_delay(0.2, 0.5)
            
            logger.warning(f"⏱️ Element not found within timeout: {selector}")
            return False
            
        except Exception as e:
            logger.error(f"Error waiting for element: {e}")
            return False
    
    @staticmethod
    async def adaptive_delay(base_delay: float = 2.0, factor: float = 1.0):
        """Adaptive delay that increases with factor"""
        delay = base_delay * factor * random.uniform(0.8, 1.2)
        await asyncio.sleep(delay)
