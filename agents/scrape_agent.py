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
    
    async def extract_all_experiences_agent(self, profile_url: str) -> List[Dict]:
        """
        UNIVERSAL AGENT-STYLE EXPERIENCE EXTRACTION
        
        This method uses an agent-like approach to:
        1. Navigate to profile's experience details page (clicks "Show all X experiences")
        2. Scroll to load ALL lazy-loaded experiences
        3. For each experience, click "X skills" buttons to get all skills
        4. Click "...see more" buttons to get full descriptions
        5. Extract all experience data with complete skills and descriptions
        
        This works universally for ALL profile structures and HTML variations.
        """
        experiences = []
        original_url = profile_url
        page = self.browser.page
        
        try:
            logger.info(f"[EXPERIENCE-AGENT] Starting universal experience extraction for: {profile_url}")
            
            # Step 1: Navigate to experience details page using multiple detection methods
            experience_page_url = await self._navigate_to_experience_page(page, profile_url)
            if experience_page_url:
                logger.info(f"[EXPERIENCE-AGENT] Navigated to experience page: {experience_page_url}")
            
            # Step 2: Scroll and load ALL experiences (handle lazy loading)
            await self._scroll_and_load_all_content(page)
            
            # Step 3: Click ALL "see more" buttons for descriptions
            await self._click_all_see_more_buttons(page)
            
            # Step 4: Extract all experiences with their skills buttons info
            experiences = await self._extract_all_experience_entries(page)
            logger.info(f"[EXPERIENCE-AGENT] Found {len(experiences)} experience entries")
            
            # Step 5: For each experience with skills button, click and extract skills
            # BUT: Skip if inline skills were already extracted (from "Show all experiences" page)
            for i, exp in enumerate(experiences):
                # Check if inline skills were already extracted
                has_inline_skills = exp.get('_has_inline_skills', False)
                existing_skills = exp.get('skills', [])
                
                # Only click skills button if no inline skills were found
                if not has_inline_skills and not existing_skills:
                    skills_button_info = exp.get('_skills_button_info')
                    if skills_button_info:
                        logger.debug(f"[EXPERIENCE-AGENT] [{i+1}/{len(experiences)}] Extracting skills for: {exp.get('title', 'Unknown')}")
                        skills = await self._click_skills_button_and_extract(page, skills_button_info)
                        if skills:
                            exp['skills'] = skills
                        # Small delay between skill extractions
                        await asyncio.sleep(0.5)
                elif has_inline_skills and existing_skills:
                    logger.debug(f"[EXPERIENCE-AGENT] [{i+1}/{len(experiences)}] Using inline skills for: {exp.get('title', 'Unknown')} ({len(existing_skills)} skills)")
                
                # Clean up internal fields if they exist
                if '_skills_button_info' in exp:
                    del exp['_skills_button_info']
                if '_has_inline_skills' in exp:
                    del exp['_has_inline_skills']
            
            # Step 6: Navigate back to original profile
            if page.url != original_url:
                try:
                    await page.goto(original_url, wait_until='domcontentloaded', timeout=15000)
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.debug(f"Could not navigate back: {e}")
            
            logger.info(f"[EXPERIENCE-AGENT] Successfully extracted {len(experiences)} experiences with skills")
            return experiences
            
        except Exception as e:
            logger.error(f"[EXPERIENCE-AGENT] Error extracting experiences: {e}")
            # Try to navigate back
            try:
                if page.url != original_url:
                    await page.goto(original_url, wait_until='domcontentloaded', timeout=10000)
            except:
                pass
            return experiences
    
    async def _navigate_to_experience_page(self, page, profile_url: str) -> Optional[str]:
        """
        UNIVERSAL navigation to experience details page.
        Uses multiple detection methods to find and click "Show all X experiences" button.
        """
        try:
            current_url = page.url
            
            # Check if already on experience page
            if '/details/experience' in current_url:
                return current_url
            
            # Method 1: Try JavaScript-based universal button detection
            experience_link = await page.evaluate("""
                () => {
                    // ULTRA-UNIVERSAL: Find any link/button that leads to experience details
                    const allClickable = document.querySelectorAll('a, button, [role="button"], [role="link"]');
                    
                    for (const el of allClickable) {
                        // Check href attribute
                        const href = el.getAttribute('href') || '';
                        const text = (el.textContent || '').toLowerCase().trim();
                        const ariaLabel = (el.getAttribute('aria-label') || '').toLowerCase();
                        const id = (el.getAttribute('id') || '').toLowerCase();
                        
                        // Priority 1: Direct href to experience details
                        if (href.includes('/details/experience')) {
                            return href;
                        }
                        
                        // Priority 2: Text/aria-label matching "show all X experiences"
                        const combinedText = text + ' ' + ariaLabel;
                        if ((combinedText.includes('show all') || combinedText.includes('see all')) && 
                            combinedText.includes('experience')) {
                            // Try to get href from this element or parent
                            if (href && href.includes('/details/experience')) {
                                return href;
                            }
                            const parentLink = el.closest('a[href*="/details/experience"]');
                            if (parentLink) {
                                return parentLink.getAttribute('href');
                            }
                        }
                        
                        // Priority 3: Check id pattern
                        if (id.includes('see-all-experience') || id.includes('navigation-index-see-all-experience')) {
                            if (href && href.includes('/details/experience')) {
                                return href;
                            }
                        }
                    }
                    
                    // Priority 4: Find ANY link to /details/experience in experience section
                    const expSection = document.querySelector('#experience, section:has(#experience)');
                    if (expSection) {
                        const expLinks = expSection.querySelectorAll('a[href*="/details/experience"]');
                        for (const link of expLinks) {
                            return link.getAttribute('href');
                        }
                    }
                    
                    // Priority 5: Construct URL from profile URL
                    const urlMatch = window.location.href.match(/\\/in\\/([^\\/]+)/);
                    if (urlMatch) {
                        return `/in/${urlMatch[1]}/details/experience/`;
                    }
                    
                    return null;
                }
            """)
            
            if experience_link:
                # Construct full URL if needed
                if not experience_link.startswith('http'):
                    experience_link = f"https://www.linkedin.com{experience_link}"
                
                # Navigate to experience page
                try:
                    await page.goto(experience_link, wait_until='domcontentloaded', timeout=30000)
                    await asyncio.sleep(2)
                    return experience_link
                except Exception as e:
                    logger.debug(f"Failed to navigate to experience page: {e}")
            
            # Method 2: Construct URL directly from profile URL
            if '/in/' in profile_url:
                profile_slug = profile_url.split('/in/')[-1].split('/')[0].split('?')[0]
                direct_url = f"https://www.linkedin.com/in/{profile_slug}/details/experience/"
                try:
                    await page.goto(direct_url, wait_until='domcontentloaded', timeout=30000)
                    await asyncio.sleep(2)
                    return direct_url
                except Exception as e:
                    logger.debug(f"Failed to navigate directly: {e}")
            
            return None
            
        except Exception as e:
            logger.debug(f"Error navigating to experience page: {e}")
            return None
    
    async def _scroll_and_load_all_content(self, page):
        """
        UNIVERSAL scroll to load all lazy-loaded content.
        Handles both main page scrolling and modal/container scrolling.
        """
        try:
            max_scroll_attempts = 25
            last_height = 0
            
            for attempt in range(max_scroll_attempts):
                # Get current scroll height
                current_height = await page.evaluate("""
                    () => {
                        return Math.max(
                            document.body.scrollHeight,
                            document.documentElement.scrollHeight
                        );
                    }
                """)
                
                # Scroll main page and all scrollable containers
                await page.evaluate("""
                    () => {
                        // Scroll main window
                        window.scrollTo(0, document.body.scrollHeight);
                        
                        // Scroll all possible containers
                        const containers = document.querySelectorAll(
                            '.scaffold-finite-scroll__content, ' +
                            '.pvs-list__container, ' +
                            '.artdeco-modal__content, ' +
                            '.pvs-modal__content, ' +
                            'main, ' +
                            '[class*="scroll"], ' +
                            '[class*="list"]'
                        );
                        
                        containers.forEach(c => {
                            try { c.scrollTop = c.scrollHeight; } catch(e) {}
                        });
                    }
                """)
                
                await asyncio.sleep(1)
                
                # Try to click "Show more results" button if present
                try:
                    show_more = await page.query_selector(
                        'button.scaffold-finite-scroll__load-button, ' +
                        'button[aria-label*="Show more"], ' +
                        'button[aria-label*="Load more"], ' +
                        'button:has-text("Show more results"), ' +
                        'button:has-text("Load more")'
                    )
                    if show_more:
                        is_visible = await show_more.is_visible()
                        if is_visible:
                            await show_more.scroll_into_view_if_needed()
                            await show_more.click()
                            await asyncio.sleep(2)
                            logger.debug(f"[SCROLL] Clicked 'Show more' button (attempt {attempt + 1})")
                            continue
                except Exception:
                    pass
                
                # Check if we've reached the bottom
                if current_height == last_height:
                    logger.debug(f"[SCROLL] Reached end of content after {attempt + 1} scrolls")
                    break
                    
                last_height = current_height
                
        except Exception as e:
            logger.debug(f"Error scrolling: {e}")
    
    async def _click_all_see_more_buttons(self, page):
        """
        UNIVERSAL click all "see more" / "...see more" buttons to expand descriptions.
        Works for any HTML structure.
        """
        try:
            max_attempts = 5
            for attempt in range(max_attempts):
                # Find ALL "see more" type buttons using multiple patterns
                see_more_clicked = await page.evaluate("""
                    () => {
                        let clicked = 0;
                        
                        // Pattern 1: Find by class name patterns
                        const selectors = [
                            '.inline-show-more-text__button',
                            '[class*="show-more"] button',
                            '[class*="see-more"] button',
                            'button[class*="show-more"]',
                            'button[class*="see-more"]',
                        ];
                        
                        for (const sel of selectors) {
                            const buttons = document.querySelectorAll(sel);
                            for (const btn of buttons) {
                                try {
                                    if (btn.offsetParent !== null) { // Is visible
                                        btn.click();
                                        clicked++;
                                    }
                                } catch(e) {}
                            }
                        }
                        
                        // Pattern 2: Find by text content
                        const allButtons = document.querySelectorAll('button, [role="button"], a');
                        for (const btn of allButtons) {
                            const text = (btn.textContent || '').toLowerCase().trim();
                            if ((text === '…see more' || text === 'see more' || text === 'show more' ||
                                 text.includes('…see more') || text === '...see more') &&
                                btn.offsetParent !== null) {
                                try {
                                    btn.click();
                                    clicked++;
                                } catch(e) {}
                            }
                        }
                        
                        // Pattern 3: Find buttons with aria-expanded="false"
                        const expandButtons = document.querySelectorAll('button[aria-expanded="false"]');
                        for (const btn of expandButtons) {
                            const text = (btn.textContent || '').toLowerCase();
                            if (text.includes('more') || text.includes('expand') || text.includes('show')) {
                                try {
                                    if (btn.offsetParent !== null) {
                                        btn.click();
                                        clicked++;
                                    }
                                } catch(e) {}
                            }
                        }
                        
                        return clicked;
                    }
                """)
                
                if see_more_clicked > 0:
                    logger.debug(f"[SEE-MORE] Clicked {see_more_clicked} 'see more' buttons (attempt {attempt + 1})")
                    await asyncio.sleep(1)
                else:
                    break
                    
        except Exception as e:
            logger.debug(f"Error clicking see more buttons: {e}")
    
    async def _extract_all_experience_entries(self, page) -> List[Dict]:
        """
        UNIVERSAL experience entry extraction using agent-style approach.
        Returns experiences with _skills_button_info for later skills extraction.
        
        ENHANCED: Now properly handles:
        1. Nested experiences (multiple positions under same company like Aamra Networks, Mark Hughes)
        2. Single skill buttons (like "Laravel" without "skill" keyword)
        3. Location vs company confusion (avoids extracting location as company)
        4. Missing experience detection (catches all experience entries)
        """
        try:
            experiences = await page.evaluate("""
                () => {
                    const experiences = [];
                    const isExperiencePage = window.location.href.includes('/details/experience');
                    
                    // ULTRA-UNIVERSAL: Find all experience items using multiple strategies
                    let expItems = [];
                    
                    // Strategy 1: On experience details page, find all list items
                    if (isExperiencePage) {
                        // Find all list items in the main content area
                        const mainContent = document.querySelector('main, .scaffold-layout__main');
                        if (mainContent) {
                            expItems = Array.from(mainContent.querySelectorAll('li')).filter(li => {
                                // Must have meaningful content
                                const text = (li.textContent || '').trim();
                                if (text.length < 20) return false;
                                
                                // Skip navigation/footer items
                                const parent = li.parentElement;
                                if (parent && (parent.getAttribute('role') === 'navigation' || 
                                    parent.classList.contains('global-nav') ||
                                    parent.closest('nav') ||
                                    parent.closest('footer'))) {
                                    return false;
                                }
                                
                                return true;
                            });
                        } else {
                            expItems = Array.from(document.querySelectorAll('li')).filter(li => {
                                const text = (li.textContent || '').trim();
                                return text.length > 20 && 
                                       !li.closest('nav') && 
                                       !li.closest('footer') &&
                                       !li.closest('[role="navigation"]');
                            });
                        }
                    } else {
                        // Strategy 2: On main profile, find experience section
                        let expSection = document.querySelector('#experience');
                        if (expSection) {
                            expSection = expSection.closest('section') || expSection.parentElement;
                        }
                        
                        if (!expSection) {
                            const allSections = document.querySelectorAll('section');
                            for (const section of allSections) {
                                const header = section.querySelector('h2');
                                if (header && header.textContent.toLowerCase().includes('experience')) {
                                    // Make sure it's not education
                                    if (!section.querySelector('#education')) {
                                        expSection = section;
                                        break;
                                    }
                                }
                            }
                        }
                        
                        if (expSection) {
                            expItems = Array.from(expSection.querySelectorAll('li')).filter(li => {
                                const text = (li.textContent || '').trim();
                                return text.length > 20 && !text.toLowerCase().includes('show all');
                            });
                        }
                    }
                    
                    // ========== ENHANCEMENT: DETECT PARENT-CHILD (NESTED) EXPERIENCE RELATIONSHIPS ==========
                    // Build a map of parent items that represent companies with multiple positions
                    // Parent items have: nested sub-components with positions, duration indicators
                    const parentCompanyMap = new Map();
                    
                    for (const item of expItems) {
                        const itemText = item.textContent || '';
                        const subComponents = item.querySelector('.pvs-entity__sub-components');
                        
                        // Check if this item is a parent company with nested positions
                        if (subComponents) {
                            const nestedItems = subComponents.querySelectorAll(':scope > ul > li, :scope > div > ul > li');
                            
                            // If there are nested items, this might be a parent company
                            if (nestedItems.length > 0) {
                                // Get potential company name from parent item
                                const entityDiv = item.querySelector('[data-view-name="profile-component-entity"]') || item;
                                const titleEl = entityDiv.querySelector('.t-bold span[aria-hidden="true"], .t-bold, h3 span[aria-hidden="true"]');
                                
                                if (titleEl) {
                                    const parentTitle = titleEl.textContent.trim();
                                    
                                    // Check if this looks like a company (has duration or nested positions)
                                    const hasDuration = itemText.match(/\\d+\\s*(yrs?|mos?)/i);
                                    const hasCompanyLink = item.querySelector('a[href*="/company/"]');
                                    
                                    if (hasDuration || hasCompanyLink || nestedItems.length > 1) {
                                        // Get company URL if available
                                        let companyUrl = 'N/A';
                                        const companyLink = entityDiv.querySelector('a[href*="/company/"]');
                                        if (companyLink) {
                                            companyUrl = companyLink.href;
                                        }
                                        
                                        // Store parent company info for each nested item
                                        for (const nestedItem of nestedItems) {
                                            parentCompanyMap.set(nestedItem, {
                                                company: parentTitle,
                                                company_url: companyUrl
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    // ========== HELPER: Check if text looks like a location (not a company) ==========
                    const looksLikeLocation = (text) => {
                        if (!text) return false;
                        const t = text.toLowerCase();
                        // Common location patterns
                        const locationPatterns = [
                            /^[a-z]+,\\s*[a-z]+$/i,  // "City, Country"
                            /dhaka/i, /bangladesh/i, /india/i, /usa/i, /uk/i,
                            /remote/i, /on-site/i, /hybrid/i,
                            /california/i, /new york/i, /london/i, /singapore/i,
                            /district/i, /province/i, /state/i
                        ];
                        // Check if it matches location patterns AND doesn't look like a company
                        const isLocationPattern = locationPatterns.some(p => p.test(t));
                        const hasCompanyIndicators = t.includes('ltd') || t.includes('inc') || 
                            t.includes('corp') || t.includes('company') || t.includes('llc') ||
                            t.includes('limited') || t.includes('pvt') || t.includes('private') ||
                            t.includes('group') || t.includes('solutions') || t.includes('technologies') ||
                            t.includes('services') || t.includes('institute') || t.includes('academy');
                        
                        return isLocationPattern && !hasCompanyIndicators;
                    };
                    
                    // Process each experience item
                    for (const item of expItems) {
                        const exp = {
                            title: 'N/A',
                            company: 'N/A',
                            employment_type: 'N/A',
                            dates: 'N/A',
                            start_date: 'N/A',
                            end_date: 'N/A',
                            duration: 'N/A',
                            location: 'N/A',
                            work_type: 'N/A',
                            skills: [],
                            description: 'N/A',
                            company_url: 'N/A',
                            accomplishments: [],
                            _skills_button_info: null,
                            _single_skill_buttons: []  // ENHANCEMENT: Track single skill buttons
                        };
                        
                        const itemText = item.textContent || '';
                        
                        // Skip education entries - ENHANCED to avoid filtering employment at institutes
                        const itemLower = itemText.toLowerCase();
                        
                        // Check for clear education indicators (degree, GPA, field of study)
                        const hasEducationIndicators = (
                            itemLower.includes('bachelor') || itemLower.includes('master') ||
                            itemLower.includes('degree') || itemLower.includes('phd') ||
                            itemLower.includes('diploma') || itemLower.includes('gpa') ||
                            itemLower.includes('graduated') || itemLower.includes('honors')
                        );
                        
                        // Check for employment indicators (job titles, employment types)
                        const hasEmploymentIndicators = (
                            itemLower.includes('developer') || itemLower.includes('engineer') ||
                            itemLower.includes('manager') || itemLower.includes('designer') ||
                            itemLower.includes('analyst') || itemLower.includes('consultant') ||
                            itemLower.includes('director') || itemLower.includes('lead') ||
                            itemLower.includes('specialist') || itemLower.includes('trainer') ||
                            itemLower.includes('instructor') || itemLower.includes('mentor') ||
                            itemLower.includes('full-time') || itemLower.includes('part-time') ||
                            itemLower.includes('internship') || itemLower.includes('contract') ||
                            itemLower.includes('apprenticeship') || itemLower.includes('freelance')
                        );
                        
                        // Only skip if it's clearly education (has education indicators AND no employment indicators)
                        const isPureEducation = (
                            (itemLower.includes('university') || itemLower.includes('college') || 
                             (itemLower.includes('school') && !hasEmploymentIndicators)) &&
                            !hasEmploymentIndicators
                        ) || (hasEducationIndicators && !hasEmploymentIndicators);
                        
                        if (isPureEducation) {
                            continue;
                        }
                        
                        // Find profile entity div (if exists)
                        let entityDiv = item.querySelector('[data-view-name="profile-component-entity"]') || item;
                        
                        // Extract title - first bold text
                        const titleEl = entityDiv.querySelector('.t-bold span[aria-hidden="true"], .t-bold, h3 span[aria-hidden="true"]');
                        if (titleEl) {
                            exp.title = titleEl.textContent.trim();
                        }
                        
                        // Extract company and employment type
                        const normalSpans = entityDiv.querySelectorAll('span.t-14.t-normal > span[aria-hidden="true"], span[class*="normal"] > span[aria-hidden="true"]');
                        for (const span of normalSpans) {
                            const text = span.textContent.trim();
                            // Skip if it's dates
                            if (text.includes(' - ') && /\\d{4}/.test(text)) continue;
                            
                            // ENHANCEMENT: Skip if it looks like a location
                            if (looksLikeLocation(text)) continue;
                            
                            if (text.includes('·')) {
                                const parts = text.split('·').map(p => p.trim());
                                if (!exp.company || exp.company === 'N/A') {
                                    // ENHANCEMENT: Validate that first part is not a location
                                    if (!looksLikeLocation(parts[0])) {
                                        exp.company = parts[0] || 'N/A';
                                    }
                                }
                                if (parts[1] && (parts[1].includes('time') || parts[1].includes('Intern') || 
                                    parts[1].includes('Contract') || parts[1].includes('Self'))) {
                                    exp.employment_type = parts[1];
                                }
                            } else if (!exp.company || exp.company === 'N/A') {
                                if (text.length > 2 && text.length < 200 && !looksLikeLocation(text)) {
                                    exp.company = text;
                                }
                            }
                            if (exp.company && exp.company !== 'N/A') break;
                        }
                        
                        // ========== ENHANCEMENT: Check parent company for nested positions ==========
                        // If this item is nested under a parent company, use parent's company info
                        if ((exp.company === 'N/A' || looksLikeLocation(exp.company)) && parentCompanyMap.has(item)) {
                            const parentInfo = parentCompanyMap.get(item);
                            exp.company = parentInfo.company;
                            if (parentInfo.company_url && parentInfo.company_url !== 'N/A') {
                                exp.company_url = parentInfo.company_url;
                            }
                        }
                        
                        // ========== ENHANCEMENT: For nested items, also check ancestor items for company ==========
                        if (exp.company === 'N/A' || looksLikeLocation(exp.company)) {
                            // Walk up the DOM to find parent experience item with company info
                            let parentLi = item.parentElement?.closest('li');
                            let depth = 0;
                            while (parentLi && depth < 5) {
                                const parentEntity = parentLi.querySelector('[data-view-name="profile-component-entity"]') || parentLi;
                                const parentTitleEl = parentEntity.querySelector('.t-bold span[aria-hidden="true"]');
                                const parentCompanyLink = parentEntity.querySelector('a[href*="/company/"]');
                                
                                if (parentTitleEl || parentCompanyLink) {
                                    // Check if parent has nested items (indicates it's a company)
                                    const parentSubComponents = parentLi.querySelector('.pvs-entity__sub-components');
                                    if (parentSubComponents) {
                                        const nestedInParent = parentSubComponents.querySelectorAll(':scope > ul > li');
                                        if (nestedInParent.length > 0) {
                                            if (parentTitleEl) {
                                                const parentTitle = parentTitleEl.textContent.trim();
                                                if (!looksLikeLocation(parentTitle)) {
                                                    exp.company = parentTitle;
                                                }
                                            }
                                            if (parentCompanyLink) {
                                                exp.company_url = parentCompanyLink.href;
                                            }
                                            break;
                                        }
                                    }
                                }
                                parentLi = parentLi.parentElement?.closest('li');
                                depth++;
                            }
                        }
                        
                        // Extract dates and duration
                        const captionEl = entityDiv.querySelector('.pvs-entity__caption-wrapper[aria-hidden="true"], [class*="caption"][aria-hidden="true"]');
                        if (captionEl) {
                            const datesText = captionEl.textContent.trim();
                            exp.dates = datesText;
                            
                            // Parse dates
                            const dateMatch = datesText.match(/^(.+?)\\s*[-–]\\s*(.+?)(?:\\s*·\\s*(.+))?$/);
                            if (dateMatch) {
                                exp.start_date = dateMatch[1].trim();
                                exp.end_date = dateMatch[2].trim();
                                if (dateMatch[3]) exp.duration = dateMatch[3].trim();
                            }
                        }
                        
                        // Extract location
                        const lightSpans = entityDiv.querySelectorAll('span.t-14.t-normal.t-black--light > span[aria-hidden="true"], span[class*="light"] > span[aria-hidden="true"]');
                        for (const span of lightSpans) {
                            const text = span.textContent.trim();
                            if (text.includes(' - ') || /\\d{4}/.test(text)) continue;
                            if (text.length > 2 && text.length < 200 && exp.location === 'N/A') {
                                exp.location = text;
                                // Extract work type
                                if (text.toLowerCase().includes('remote')) exp.work_type = 'Remote';
                                else if (text.toLowerCase().includes('hybrid')) exp.work_type = 'Hybrid';
                                else if (text.toLowerCase().includes('on-site')) exp.work_type = 'On-site';
                                break;
                            }
                        }
                        
                        // ========== ENHANCEMENT: Final company validation ==========
                        // If company still looks like a location, try to get from company link
                        if (looksLikeLocation(exp.company)) {
                            const companyLink = entityDiv.querySelector('a[href*="/company/"]');
                            if (companyLink) {
                                // Try to get company name from link's aria-label or title
                                const ariaLabel = companyLink.getAttribute('aria-label') || '';
                                const linkTitle = companyLink.getAttribute('title') || '';
                                const linkText = companyLink.textContent.trim();
                                
                                // Use whichever has valid company info
                                if (ariaLabel && !looksLikeLocation(ariaLabel)) {
                                    exp.company = ariaLabel;
                                } else if (linkTitle && !looksLikeLocation(linkTitle)) {
                                    exp.company = linkTitle;
                                } else if (linkText && !looksLikeLocation(linkText)) {
                                    exp.company = linkText;
                                } else {
                                    // Extract from URL as last resort
                                    const href = companyLink.getAttribute('href') || '';
                                    const companyMatch = href.match(/\\/company\\/([^\\/\\?]+)/);
                                    if (companyMatch) {
                                        // This will be company ID, might need resolution
                                        exp.company = 'N/A'; // Better to show N/A than location
                                    }
                                }
                            } else if (exp.location === 'N/A' && exp.company !== 'N/A') {
                                // The "company" is actually location, swap them
                                exp.location = exp.company;
                                exp.company = 'N/A';
                            }
                        }
                        
                        // Extract description from sub-components
                        const subComponents = entityDiv.querySelector('.pvs-entity__sub-components');
                        if (subComponents) {
                            // Find description (longer text that's not skills)
                            const allSpans = subComponents.querySelectorAll('span[aria-hidden="true"]');
                            for (const span of allSpans) {
                                const text = span.textContent.trim();
                                if (text.length > 50 && !text.includes('skill') && 
                                    !text.includes('Skills:') && !text.includes('+')) {
                                    exp.description = text;
                                    break;
                                }
                            }
                        }
                        
                        // Get company URL
                        const companyLink = entityDiv.querySelector('a[href*="/company/"]');
                        if (companyLink && exp.company_url === 'N/A') {
                            exp.company_url = companyLink.href;
                        }
                        
                        // CRITICAL: Extract skills from inline text (when "Show all experiences" is clicked)
                        // This handles cases where skills are shown as "Skills: CodeIgniter · Laravel · jQuery..."
                        // UNIVERSAL: Search for "Skills:" pattern in the entire item text
                        const fullItemText = item.textContent || '';
                        const skillsMatch = fullItemText.match(/Skills?:\\s*([^\\n]+)/i);
                        if (skillsMatch) {
                            const skillsText = skillsMatch[1].trim();
                            // Split by · (middle dot) or comma
                            const skillsArray = skillsText.split(/[·,]/).map(s => s.trim()).filter(s => {
                                // Filter out invalid entries
                                const skill = s.trim();
                                return skill.length > 0 && 
                                       skill.length <= 60 && 
                                       !skill.toLowerCase().includes('and +') &&
                                       !skill.toLowerCase().includes('skill') &&
                                       !skill.match(/^\\+\\d+$/); // Not just "+5"
                            });
                            
                            if (skillsArray.length > 0) {
                                exp.skills = skillsArray;
                                // Mark that we found inline skills, so we don't need to click button
                                exp._has_inline_skills = true;
                            }
                        }
                        
                        // Also check in sub-components for "Skills:" text
                        if (subComponents && exp.skills.length === 0) {
                            const subText = subComponents.textContent || '';
                            const subSkillsMatch = subText.match(/Skills?:\\s*([^\\n]+)/i);
                            if (subSkillsMatch) {
                                const skillsText = subSkillsMatch[1].trim();
                                const skillsArray = skillsText.split(/[·,]/).map(s => s.trim()).filter(s => {
                                    const skill = s.trim();
                                    return skill.length > 0 && 
                                           skill.length <= 60 && 
                                           !skill.toLowerCase().includes('and +') &&
                                           !skill.toLowerCase().includes('skill') &&
                                           !skill.match(/^\\+\\d+$/);
                                });
                                
                                if (skillsArray.length > 0) {
                                    exp.skills = skillsArray;
                                    exp._has_inline_skills = true;
                                }
                            }
                        }
                        
                        // CRITICAL: Find skills button info for later clicking (only if no inline skills found)
                        // Only look for skills button if we didn't find inline skills
                        if (!exp._has_inline_skills || exp.skills.length === 0) {
                            // Use multiple strategies to find skills button
                            let skillsButton = null;
                            
                            // Method 1: data-field attribute
                            skillsButton = item.querySelector('a[data-field="position_contextual_skills_see_details"]');
                            
                            // Method 2: href containing skill-associations
                            if (!skillsButton) {
                                skillsButton = item.querySelector('a[href*="skill-associations"]');
                            }
                            
                            // Method 3: href containing position_contextual_skills
                            if (!skillsButton) {
                                skillsButton = item.querySelector('a[href*="position_contextual_skills"]');
                            }
                            
                            // Method 4: Text-based detection
                            if (!skillsButton) {
                                const allLinks = item.querySelectorAll('a');
                                for (const link of allLinks) {
                                    const text = (link.textContent || '').toLowerCase();
                                    const href = link.getAttribute('href') || '';
                                    if ((text.includes('skill') && (text.includes('+') || /\\d+/.test(text))) ||
                                        href.includes('skill')) {
                                        skillsButton = link;
                                        break;
                                    }
                                }
                            }
                            
                            if (skillsButton) {
                                const rect = skillsButton.getBoundingClientRect();
                                exp._skills_button_info = {
                                    href: skillsButton.getAttribute('href') || skillsButton.href,
                                    text: skillsButton.textContent.trim(),
                                    x: rect.x + rect.width / 2,
                                    y: rect.y + rect.height / 2,
                                    selector: skillsButton.getAttribute('data-field') ? 
                                        'a[data-field="position_contextual_skills_see_details"]' : 
                                        'a[href*="skill-associations"]'
                                };
                                
                                // Extract preview skills from button text (only if no inline skills)
                                if (!exp._has_inline_skills) {
                                    const btnText = skillsButton.textContent.trim();
                                    if (btnText && !btnText.toLowerCase().includes('skill')) {
                                        // Text like "Skill1, Skill2 and +X skills"
                                        const previewMatch = btnText.match(/(.+?)(?:\\s+and\\s+\\+\\d+\\s+skill)?$/i);
                                        if (previewMatch) {
                                            const previewSkills = previewMatch[1].split(',').map(s => s.trim()).filter(s => s.length > 0 && s.length < 60);
                                            if (previewSkills.length > 0 && exp.skills.length === 0) {
                                                exp.skills = previewSkills; // Initial preview
                                            }
                                        }
                                    }
                                }
                            }
                            
                            // ========== ENHANCEMENT: Detect single skill buttons (like "Laravel") ==========
                            // These are small buttons/pills that show a single skill name without "skill" keyword
                            if (!skillsButton && exp.skills.length === 0) {
                                const allButtons = item.querySelectorAll('button, a.artdeco-pill, span.artdeco-pill, [class*="pill"], [class*="tag"], [class*="skill"]');
                                const singleSkillButtons = [];
                                
                                for (const btn of allButtons) {
                                    const btnText = btn.textContent.trim();
                                    // Single skill buttons are short (1-60 chars), single word or tech name
                                    // Examples: "Laravel", "React.js", "Python", "AWS"
                                    if (btnText.length > 0 && btnText.length <= 60 &&
                                        !btnText.includes('Show') && !btnText.includes('See') &&
                                        !btnText.includes('More') && !btnText.includes('all') &&
                                        !btnText.includes('+') && !btnText.match(/^\\d+$/) &&
                                        !btnText.includes('skill') && !btnText.includes('Skill') &&
                                        !looksLikeLocation(btnText)) {
                                        
                                        // Additional validation: should look like a tech/skill name
                                        const looksLikeSkill = /^[A-Za-z][A-Za-z0-9.\\-\\+#\\s]{0,59}$/.test(btnText);
                                        if (looksLikeSkill) {
                                            singleSkillButtons.push({
                                                text: btnText,
                                                element_class: btn.className || ''
                                            });
                                        }
                                    }
                                }
                                
                                // If we found single skill buttons, add them to skills
                                if (singleSkillButtons.length > 0 && singleSkillButtons.length <= 5) {
                                    exp.skills = singleSkillButtons.map(s => s.text);
                                    exp._has_inline_skills = true;
                                }
                            }
                        }
                        
                        // Validation: Must have at least title or company or dates
                        const hasValidData = (exp.title && exp.title !== 'N/A' && exp.title.length > 2) ||
                                            (exp.company && exp.company !== 'N/A') ||
                                            (exp.dates && exp.dates !== 'N/A');
                        
                        if (hasValidData) {
                            // Clean up internal fields before adding
                            delete exp._single_skill_buttons;
                            experiences.push(exp);
                        }
                    }
                    
                    return experiences;
                }
            """)
            
            # ========== ENHANCEMENT: Clean up duplicate text patterns in titles ==========
            # LinkedIn sometimes has duplicate text due to aria-hidden attributes
            # e.g., "CEOCEO" → "CEO", "Senior DeveloperSenior Developer" → "Senior Developer"
            def clean_duplicate_text(text):
                if not text or text == 'N/A':
                    return text
                
                text = text.strip()
                length = len(text)
                
                # Check if text is a perfect duplicate (first half = second half)
                if length >= 2 and length % 2 == 0:
                    half = length // 2
                    if text[:half] == text[half:]:
                        return text[:half]
                
                # Check for duplicate with space separator
                # e.g., "CEO CEO" → "CEO"
                parts = text.split()
                if len(parts) >= 2 and len(parts) % 2 == 0:
                    half = len(parts) // 2
                    if parts[:half] == parts[half:]:
                        return ' '.join(parts[:half])
                
                return text
            
            # Deduplicate experiences
            seen = set()
            unique_experiences = []
            for exp in experiences:
                # Clean up duplicate text patterns
                if exp.get('title'):
                    exp['title'] = clean_duplicate_text(exp['title'])
                if exp.get('company'):
                    exp['company'] = clean_duplicate_text(exp['company'])
                
                key = f"{exp.get('title', '')}|{exp.get('company', '')}|{exp.get('start_date', '')}"
                if key not in seen:
                    seen.add(key)
                    unique_experiences.append(exp)
            
            return unique_experiences
            
        except Exception as e:
            logger.error(f"Error extracting experience entries: {e}")
            return []
    
    async def _click_skills_button_and_extract(self, page, skills_button_info: Dict) -> List[str]:
        """
        UNIVERSAL skills extraction by clicking skills button and extracting from modal.
        """
        skills = []
        try:
            href = skills_button_info.get('href', '')
            selector = skills_button_info.get('selector', '')
            
            # Method 1: Try to click the button directly
            try:
                # Find button by multiple methods
                skills_button = None
                
                if selector:
                    skills_button = await page.query_selector(selector)
                
                if not skills_button and href:
                    skills_button = await page.query_selector(f'a[href="{href}"]')
                
                if not skills_button:
                    skills_button = await page.query_selector('a[data-field="position_contextual_skills_see_details"]')
                
                if not skills_button and href:
                    # Try partial href match
                    href_key = href.split('/')[-1].split('?')[0] if '/' in href else href
                    if href_key:
                        skills_button = await page.query_selector(f'a[href*="{href_key}"]')
                
                if skills_button:
                    await skills_button.scroll_into_view_if_needed()
                    await asyncio.sleep(0.5)
                    await skills_button.click()
                    await asyncio.sleep(2)
                    
                    # Extract skills from modal
                    skills = await self._extract_skills_from_modal(page)
                    
                    # Close modal
                    try:
                        await page.keyboard.press('Escape')
                        await asyncio.sleep(0.5)
                    except:
                        # Try close button
                        close_btn = await page.query_selector('.artdeco-modal__dismiss, button[aria-label*="Close"], button[aria-label*="Dismiss"]')
                        if close_btn:
                            await close_btn.click()
                            await asyncio.sleep(0.5)
                    
            except Exception as e:
                logger.debug(f"Error clicking skills button: {e}")
            
            # Method 2: If clicking failed, try navigating to URL
            if not skills and href:
                try:
                    full_url = href if href.startswith('http') else f"https://www.linkedin.com{href}"
                    original_url = page.url
                    
                    await page.goto(full_url, wait_until='domcontentloaded', timeout=20000)
                    await asyncio.sleep(2)
                    
                    skills = await self._extract_skills_from_modal(page)
                    
                    # Navigate back
                    await page.goto(original_url, wait_until='domcontentloaded', timeout=15000)
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.debug(f"Error navigating to skills page: {e}")
            
            return skills
            
        except Exception as e:
            logger.error(f"Error extracting skills: {e}")
            return skills
    
    async def _extract_skills_from_modal(self, page) -> List[str]:
        """
        UNIVERSAL skills extraction from open modal or skills page.
        Handles multiple structures including modal popups and inline skills.
        """
        skills = []
        try:
            # Wait a bit for modal to fully load
            await asyncio.sleep(1)
            
            # Scroll modal to load all skills
            for _ in range(5):
                await page.evaluate("""
                    () => {
                        const modal = document.querySelector('.artdeco-modal__content, .pvs-modal__content');
                        if (modal) modal.scrollTop = modal.scrollHeight;
                        
                        const container = document.querySelector('.scaffold-finite-scroll__content, .pvs-list__container');
                        if (container) container.scrollTop = container.scrollHeight;
                        
                        window.scrollTo(0, document.body.scrollHeight);
                    }
                """)
                await asyncio.sleep(0.5)
            
            # Extract skills using multiple methods
            skills_list = await page.evaluate("""
                () => {
                    const skills = [];
                    const skipTexts = ['learn more', 'skills', 'discover', 'endorsed', 'endorsement', 
                                      'see all', 'show all', 'people', 'connection', 'message', 'follow'];
                    
                    // Method 1: Standard modal skills
                    let skillElements = document.querySelectorAll(
                        '.artdeco-modal [data-view-name="profile-component-entity"] .mr1.t-bold span[aria-hidden="true"], ' +
                        '.pvs-modal [data-view-name="profile-component-entity"] .mr1.t-bold span[aria-hidden="true"], ' +
                        '.artdeco-modal [data-view-name="profile-component-entity"] .t-bold span[aria-hidden="true"], ' +
                        '.pvs-modal [data-view-name="profile-component-entity"] .t-bold span[aria-hidden="true"]'
                    );
                    
                    for (const el of skillElements) {
                        const text = el.textContent.trim();
                        if (text.length > 0 && text.length <= 60 && 
                            !skipTexts.some(s => text.toLowerCase().includes(s))) {
                            skills.push(text);
                        }
                    }
                    
                    // Method 2: Skills from list items in modal
                    if (skills.length === 0) {
                        const listItems = document.querySelectorAll('.artdeco-modal li, .pvs-modal li, main li');
                        for (const item of listItems) {
                            const entity = item.querySelector('[data-view-name="profile-component-entity"]');
                            if (entity) {
                                const titleEl = entity.querySelector('.mr1.t-bold span[aria-hidden="true"], .t-bold span[aria-hidden="true"]');
                                if (titleEl) {
                                    const text = titleEl.textContent.trim();
                                    if (text.length > 0 && text.length <= 60 && 
                                        !skipTexts.some(s => text.toLowerCase().includes(s))) {
                                        skills.push(text);
                                    }
                                }
                            }
                        }
                    }
                    
                    // Method 3: Skills from "Skills:" inline text
                    if (skills.length === 0) {
                        const allText = document.body.textContent || '';
                        const skillsMatch = allText.match(/Skills?:\\s*([^\\n]+)/i);
                        if (skillsMatch) {
                            const skillsStr = skillsMatch[1];
                            const skillArray = skillsStr.split(/[·,]/).map(s => s.trim()).filter(s => 
                                s.length > 0 && s.length <= 60 && !skipTexts.some(skip => s.toLowerCase().includes(skip))
                            );
                            skills.push(...skillArray);
                        }
                    }
                    
                    return [...new Set(skills)]; // Deduplicate
                }
            """)
            
            if skills_list:
                skills = skills_list
                
        except Exception as e:
            logger.debug(f"Error extracting skills from modal: {e}")
        
        return skills
    
    # ========== EDUCATION AGENT METHODS ==========
    
    async def extract_all_education_agent(self, profile_url: str) -> List[Dict]:
        """
        UNIVERSAL AGENT-STYLE EDUCATION EXTRACTION
        
        This method uses an agent-like approach to:
        1. Navigate to profile's education details page (clicks "Show all X education")
        2. Scroll to load ALL lazy-loaded education entries
        3. For each education, click "X skills" buttons to get all skills
        4. Click "...see more" buttons to get full descriptions
        5. Extract all education data with complete skills and descriptions
        
        This works universally for ALL profile structures and HTML variations.
        """
        education_list = []
        original_url = profile_url
        page = self.browser.page
        
        try:
            logger.info(f"[EDUCATION-AGENT] Starting universal education extraction for: {profile_url}")
            
            # Step 1: Navigate to education details page using multiple detection methods
            education_page_url = await self._navigate_to_education_page(page, profile_url)
            if education_page_url:
                logger.info(f"[EDUCATION-AGENT] Navigated to education page: {education_page_url}")
            
            # Step 2: Scroll and load ALL education entries (handle lazy loading)
            await self._scroll_and_load_all_education(page)
            
            # Step 3: Click ALL "see more" buttons for descriptions
            await self._click_all_education_see_more_buttons(page)
            
            # Step 4: Extract all education entries with their skills buttons info
            education_list = await self._extract_all_education_entries(page)
            logger.info(f"[EDUCATION-AGENT] Found {len(education_list)} education entries")
            
            # Step 5: For each education with skills button, click and extract skills
            for i, edu in enumerate(education_list):
                skills_button_info = edu.get('_skills_button_info')
                if skills_button_info:
                    logger.debug(f"[EDUCATION-AGENT] [{i+1}/{len(education_list)}] Extracting skills for: {edu.get('school', 'Unknown')}")
                    skills = await self._click_education_skills_button_and_extract(page, skills_button_info)
                    if skills:
                        edu['skills'] = skills
                    # Small delay between skill extractions
                    await asyncio.sleep(0.5)
                
                # Clean up internal field if it exists
                if '_skills_button_info' in edu:
                    del edu['_skills_button_info']
            
            # Step 6: Navigate back to original profile
            if page.url != original_url:
                try:
                    await page.goto(original_url, wait_until='domcontentloaded', timeout=15000)
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.debug(f"Could not navigate back: {e}")
            
            logger.info(f"[EDUCATION-AGENT] Successfully extracted {len(education_list)} education entries with skills")
            return education_list
            
        except Exception as e:
            logger.error(f"[EDUCATION-AGENT] Error extracting education: {e}")
            # Try to navigate back
            try:
                if page.url != original_url:
                    await page.goto(original_url, wait_until='domcontentloaded', timeout=10000)
            except:
                pass
            return education_list
    
    async def _navigate_to_education_page(self, page, profile_url: str) -> Optional[str]:
        """
        UNIVERSAL navigation to education details page.
        Uses multiple detection methods to find and click "Show all X education" button.
        """
        try:
            current_url = page.url
            
            # Check if already on education page
            if '/details/education' in current_url:
                return current_url
            
            # Method 1: Try JavaScript-based universal button detection
            education_link = await page.evaluate("""
                () => {
                    // ULTRA-UNIVERSAL: Find any link/button that leads to education details
                    const allClickable = document.querySelectorAll('a, button, [role="button"], [role="link"]');
                    
                    for (const el of allClickable) {
                        // Check href attribute
                        const href = el.getAttribute('href') || '';
                        const text = (el.textContent || '').toLowerCase().trim();
                        const ariaLabel = (el.getAttribute('aria-label') || '').toLowerCase();
                        const id = (el.getAttribute('id') || '').toLowerCase();
                        
                        // Priority 1: Direct href to education details
                        if (href.includes('/details/education')) {
                            return href;
                        }
                        
                        // Priority 2: Text/aria-label matching "show all X education"
                        const combinedText = text + ' ' + ariaLabel;
                        if ((combinedText.includes('show all') || combinedText.includes('see all')) && 
                            combinedText.includes('education')) {
                            // Try to get href from this element or parent
                            if (href && href.includes('/details/education')) {
                                return href;
                            }
                            const parentLink = el.closest('a[href*="/details/education"]');
                            if (parentLink) {
                                return parentLink.getAttribute('href');
                            }
                        }
                        
                        // Priority 3: Check id pattern
                        if (id.includes('see-all-education') || id.includes('navigation-index-see-all-education')) {
                            if (href && href.includes('/details/education')) {
                                return href;
                            }
                        }
                    }
                    
                    // Priority 4: Find ANY link to /details/education in education section
                    const eduSection = document.querySelector('#education, section:has(#education)');
                    if (eduSection) {
                        const eduLinks = eduSection.querySelectorAll('a[href*="/details/education"]');
                        for (const link of eduLinks) {
                            return link.getAttribute('href');
                        }
                    }
                    
                    // Priority 5: Construct URL from profile URL
                    const urlMatch = window.location.href.match(/\\/in\\/([^\\/]+)/);
                    if (urlMatch) {
                        return `/in/${urlMatch[1]}/details/education/`;
                    }
                    
                    return null;
                }
            """)
            
            if education_link:
                # Construct full URL if needed
                if not education_link.startswith('http'):
                    education_link = f"https://www.linkedin.com{education_link}"
                
                # Navigate to education page
                try:
                    await page.goto(education_link, wait_until='domcontentloaded', timeout=30000)
                    await asyncio.sleep(2)
                    return education_link
                except Exception as e:
                    logger.debug(f"Failed to navigate to education page: {e}")
            
            # Method 2: Construct URL directly from profile URL
            if '/in/' in profile_url:
                profile_slug = profile_url.split('/in/')[-1].split('/')[0].split('?')[0]
                direct_url = f"https://www.linkedin.com/in/{profile_slug}/details/education/"
                try:
                    await page.goto(direct_url, wait_until='domcontentloaded', timeout=30000)
                    await asyncio.sleep(2)
                    return direct_url
                except Exception as e:
                    logger.debug(f"Failed to navigate directly to education: {e}")
            
            return None
            
        except Exception as e:
            logger.debug(f"Error navigating to education page: {e}")
            return None
    
    async def _scroll_and_load_all_education(self, page):
        """
        UNIVERSAL scroll to load all lazy-loaded education content.
        """
        try:
            max_scroll_attempts = 15
            last_height = 0
            
            for attempt in range(max_scroll_attempts):
                # Get current scroll height
                current_height = await page.evaluate("""
                    () => {
                        return Math.max(
                            document.body.scrollHeight,
                            document.documentElement.scrollHeight
                        );
                    }
                """)
                
                # Scroll main page and all scrollable containers
                await page.evaluate("""
                    () => {
                        // Scroll main window
                        window.scrollTo(0, document.body.scrollHeight);
                        
                        // Scroll all possible containers
                        const containers = document.querySelectorAll(
                            '.scaffold-finite-scroll__content, ' +
                            '.pvs-list__container, ' +
                            '.artdeco-modal__content, ' +
                            '.pvs-modal__content, ' +
                            'main, ' +
                            '[class*="scroll"], ' +
                            '[class*="list"]'
                        );
                        
                        containers.forEach(c => {
                            try { c.scrollTop = c.scrollHeight; } catch(e) {}
                        });
                    }
                """)
                
                await asyncio.sleep(1)
                
                # Try to click "Show more results" button if present
                try:
                    show_more = await page.query_selector(
                        'button.scaffold-finite-scroll__load-button, ' +
                        'button[aria-label*="Show more"], ' +
                        'button[aria-label*="Load more"], ' +
                        'button:has-text("Show more results"), ' +
                        'button:has-text("Load more")'
                    )
                    if show_more:
                        is_visible = await show_more.is_visible()
                        if is_visible:
                            await show_more.scroll_into_view_if_needed()
                            await show_more.click()
                            await asyncio.sleep(2)
                            logger.debug(f"[EDUCATION-SCROLL] Clicked 'Show more' button (attempt {attempt + 1})")
                            continue
                except Exception:
                    pass
                
                # Check if we've reached the bottom
                if current_height == last_height:
                    logger.debug(f"[EDUCATION-SCROLL] Reached end of content after {attempt + 1} scrolls")
                    break
                    
                last_height = current_height
                
        except Exception as e:
            logger.debug(f"Error scrolling education: {e}")
    
    async def _click_all_education_see_more_buttons(self, page):
        """
        UNIVERSAL click all "see more" buttons to expand education descriptions.
        """
        try:
            max_attempts = 5
            for attempt in range(max_attempts):
                # Find ALL "see more" type buttons
                see_more_clicked = await page.evaluate("""
                    () => {
                        let clicked = 0;
                        
                        // Pattern 1: Find by class name patterns
                        const selectors = [
                            '.inline-show-more-text__button',
                            '[class*="show-more"] button',
                            '[class*="see-more"] button',
                            'button[class*="show-more"]',
                            'button[class*="see-more"]',
                        ];
                        
                        for (const sel of selectors) {
                            const buttons = document.querySelectorAll(sel);
                            for (const btn of buttons) {
                                try {
                                    if (btn.offsetParent !== null) {
                                        btn.click();
                                        clicked++;
                                    }
                                } catch(e) {}
                            }
                        }
                        
                        // Pattern 2: Find by text content
                        const allButtons = document.querySelectorAll('button, [role="button"], a');
                        for (const btn of allButtons) {
                            const text = (btn.textContent || '').toLowerCase().trim();
                            if ((text === '…see more' || text === 'see more' || text === 'show more' ||
                                 text.includes('…see more') || text === '...see more') &&
                                btn.offsetParent !== null) {
                                try {
                                    btn.click();
                                    clicked++;
                                } catch(e) {}
                            }
                        }
                        
                        return clicked;
                    }
                """)
                
                if see_more_clicked > 0:
                    logger.debug(f"[EDUCATION-SEE-MORE] Clicked {see_more_clicked} 'see more' buttons (attempt {attempt + 1})")
                    await asyncio.sleep(1)
                else:
                    break
                    
        except Exception as e:
            logger.debug(f"Error clicking education see more buttons: {e}")
    
    async def _extract_all_education_entries(self, page) -> List[Dict]:
        """
        UNIVERSAL education entry extraction using agent-style approach.
        Returns education entries with _skills_button_info for later skills extraction.
        """
        try:
            education_list = await page.evaluate("""
                () => {
                    const education = [];
                    const isEducationPage = window.location.href.includes('/details/education');
                    
                    // ULTRA-UNIVERSAL: Find all education items using multiple strategies
                    let eduItems = [];
                    
                    // Strategy 1: On education details page, find all list items
                    if (isEducationPage) {
                        const mainContent = document.querySelector('main, .scaffold-layout__main');
                        if (mainContent) {
                            eduItems = Array.from(mainContent.querySelectorAll('li')).filter(li => {
                                const text = (li.textContent || '').trim();
                                if (text.length < 20) return false;
                                
                                const parent = li.parentElement;
                                if (parent && (parent.getAttribute('role') === 'navigation' || 
                                    parent.classList.contains('global-nav') ||
                                    li.closest('nav') ||
                                    li.closest('footer'))) {
                                    return false;
                                }
                                
                                return true;
                            });
                        } else {
                            eduItems = Array.from(document.querySelectorAll('li')).filter(li => {
                                const text = (li.textContent || '').trim();
                                return text.length > 20 && 
                                       !li.closest('nav') && 
                                       !li.closest('footer') &&
                                       !li.closest('[role="navigation"]');
                            });
                        }
                    } else {
                        // Strategy 2: On main profile, find education section
                        let eduSection = document.querySelector('#education');
                        if (eduSection) {
                            eduSection = eduSection.closest('section') || eduSection.parentElement;
                        }
                        
                        if (!eduSection) {
                            const allSections = document.querySelectorAll('section');
                            for (const section of allSections) {
                                const header = section.querySelector('h2');
                                if (header && header.textContent.toLowerCase().includes('education')) {
                                    if (!section.querySelector('#experience')) {
                                        eduSection = section;
                                        break;
                                    }
                                }
                            }
                        }
                        
                        if (eduSection) {
                            eduItems = Array.from(eduSection.querySelectorAll('li')).filter(li => {
                                const text = (li.textContent || '').trim();
                                return text.length > 20 && !text.toLowerCase().includes('show all');
                            });
                        }
                    }
                    
                    // Process each education item
                    for (const item of eduItems) {
                        const edu = {
                            school: 'N/A',
                            degree: 'N/A',
                            field_of_study: 'N/A',
                            start_date: 'N/A',
                            end_date: 'N/A',
                            dates: 'N/A',
                            duration: 'N/A',
                            grade: 'N/A',
                            description: 'N/A',
                            school_url: 'N/A',
                            activities: [],
                            skills: [],
                            _skills_button_info: null
                        };
                        
                        const itemText = item.textContent || '';
                        const itemLower = itemText.toLowerCase();
                        
                        // Skip experience entries
                        if (itemLower.includes('full-time') && !itemLower.includes('university') && !itemLower.includes('college')) {
                            continue;
                        }
                        
                        // Find profile entity div (if exists)
                        let entityDiv = item.querySelector('[data-view-name="profile-component-entity"]') || item;
                        
                        // Extract school name - first bold text
                        const schoolEl = entityDiv.querySelector('.t-bold span[aria-hidden="true"], .t-bold, h3 span[aria-hidden="true"]');
                        if (schoolEl) {
                            edu.school = schoolEl.textContent.trim();
                        }
                        
                        // Extract degree and field of study
                        const normalSpans = entityDiv.querySelectorAll('span.t-14.t-normal > span[aria-hidden="true"], span[class*="normal"] > span[aria-hidden="true"]');
                        for (const span of normalSpans) {
                            const text = span.textContent.trim();
                            // Skip if it's dates
                            if (text.includes(' - ') && /\\d{4}/.test(text)) continue;
                            
                            if (text.includes(',')) {
                                // Format: "Degree, Field of Study"
                                const parts = text.split(',').map(p => p.trim());
                                if (!edu.degree || edu.degree === 'N/A') {
                                    edu.degree = parts[0] || 'N/A';
                                }
                                if (parts[1] && (!edu.field_of_study || edu.field_of_study === 'N/A')) {
                                    edu.field_of_study = parts[1];
                                }
                            } else if (!edu.degree || edu.degree === 'N/A') {
                                if (text.length > 2 && text.length < 200) {
                                    edu.degree = text;
                                }
                            }
                            if (edu.degree && edu.degree !== 'N/A') break;
                        }
                        
                        // Extract dates
                        const captionEl = entityDiv.querySelector('.pvs-entity__caption-wrapper[aria-hidden="true"], [class*="caption"][aria-hidden="true"]');
                        if (captionEl) {
                            const datesText = captionEl.textContent.trim();
                            edu.dates = datesText;
                            
                            // Parse dates
                            const dateMatch = datesText.match(/^(.+?)\\s*[-–]\\s*(.+?)(?:\\s*·\\s*(.+))?$/);
                            if (dateMatch) {
                                edu.start_date = dateMatch[1].trim();
                                edu.end_date = dateMatch[2].trim();
                                if (dateMatch[3]) edu.duration = dateMatch[3].trim();
                            } else {
                                // Single year format
                                const yearMatch = datesText.match(/^(\\d{4})$/);
                                if (yearMatch) {
                                    edu.start_date = yearMatch[1];
                                    edu.end_date = yearMatch[1];
                                }
                            }
                        }
                        
                        // Extract grade and description from sub-components
                        const subComponents = entityDiv.querySelector('.pvs-entity__sub-components');
                        if (subComponents) {
                            const allSpans = subComponents.querySelectorAll('span[aria-hidden="true"]');
                            for (const span of allSpans) {
                                const text = span.textContent.trim();
                                // Look for grade pattern
                                if (text.toLowerCase().includes('grade') || text.toLowerCase().includes('gpa') || 
                                    text.toLowerCase().includes('cgpa') || text.toLowerCase().includes('first class') ||
                                    text.toLowerCase().includes('second class') || text.toLowerCase().includes('distinction')) {
                                    edu.grade = text;
                                } else if (text.length > 50 && !text.includes('skill') && 
                                    !text.includes('Skills:') && !text.includes('+') &&
                                    edu.description === 'N/A') {
                                    edu.description = text;
                                }
                            }
                            
                            // Look for activities
                            const activityText = subComponents.textContent || '';
                            if (activityText.toLowerCase().includes('activities') || activityText.toLowerCase().includes('societies')) {
                                const activityMatch = activityText.match(/(?:Activities|Societies)[:\\s]+([^\\n]+)/i);
                                if (activityMatch) {
                                    edu.activities = activityMatch[1].split(',').map(a => a.trim()).filter(a => a.length > 0);
                                }
                            }
                        }
                        
                        // Get school URL
                        const schoolLink = entityDiv.querySelector('a[href*="/company/"], a[href*="/school/"]');
                        if (schoolLink) {
                            edu.school_url = schoolLink.href;
                        }
                        
                        // CRITICAL: Find skills button info for later clicking
                        let skillsButton = null;
                        
                        // Method 1: data-field attribute
                        skillsButton = item.querySelector('a[data-field="education_skill_associations"]');
                        
                        // Method 2: href containing skill-associations
                        if (!skillsButton) {
                            skillsButton = item.querySelector('a[href*="skill-associations"]');
                        }
                        
                        // Method 3: Text-based detection for "X skills" buttons
                        if (!skillsButton) {
                            const allLinks = item.querySelectorAll('a');
                            for (const link of allLinks) {
                                const text = (link.textContent || '').toLowerCase();
                                const href = link.getAttribute('href') || '';
                                if ((text.includes('skill') && (text.includes('+') || /\\d+/.test(text))) ||
                                    href.includes('skill')) {
                                    skillsButton = link;
                                    break;
                                }
                            }
                        }
                        
                        if (skillsButton) {
                            const rect = skillsButton.getBoundingClientRect();
                            edu._skills_button_info = {
                                href: skillsButton.getAttribute('href') || skillsButton.href,
                                text: skillsButton.textContent.trim(),
                                x: rect.x + rect.width / 2,
                                y: rect.y + rect.height / 2,
                                selector: skillsButton.getAttribute('data-field') ? 
                                    'a[data-field="education_skill_associations"]' : 
                                    'a[href*="skill-associations"]'
                            };
                            
                            // Extract preview skills from button text
                            const btnText = skillsButton.textContent.trim();
                            if (btnText && !btnText.toLowerCase().includes('skill')) {
                                const previewMatch = btnText.match(/(.+?)(?:\\s+and\\s+\\+\\d+\\s+skill)?$/i);
                                if (previewMatch) {
                                    const previewSkills = previewMatch[1].split(',').map(s => s.trim()).filter(s => s.length > 0 && s.length < 60);
                                    if (previewSkills.length > 0) {
                                        edu.skills = previewSkills;
                                    }
                                }
                            }
                        }
                        
                        // Validation: Must have school name
                        const hasValidData = edu.school && edu.school !== 'N/A' && edu.school.length > 2;
                        
                        // Filter out noise
                        const schoolLower = edu.school.toLowerCase();
                        const isNoise = schoolLower.includes('followers') ||
                                       schoolLower.includes('members') ||
                                       schoolLower.includes('follows your') ||
                                       schoolLower.includes('viewed your') ||
                                       schoolLower === 'fiverr' ||
                                       schoolLower === 'upwork' ||
                                       /^\\d+[,\\s]*\\d+[,\\s]*followers/i.test(edu.school);
                        
                        if (hasValidData && !isNoise) {
                            education.push(edu);
                        }
                    }
                    
                    return education;
                }
            """)
            
            # Deduplicate education entries
            seen = set()
            unique_education = []
            for edu in education_list:
                key = f"{edu.get('school', '')}|{edu.get('degree', '')}|{edu.get('start_date', '')}"
                if key not in seen:
                    seen.add(key)
                    unique_education.append(edu)
            
            return unique_education
            
        except Exception as e:
            logger.error(f"Error extracting education entries: {e}")
            return []
    
    async def _click_education_skills_button_and_extract(self, page, skills_button_info: Dict) -> List[str]:
        """
        UNIVERSAL education skills extraction by clicking skills button and extracting from modal.
        """
        skills = []
        try:
            href = skills_button_info.get('href', '')
            selector = skills_button_info.get('selector', '')
            
            # Method 1: Try to click the button directly
            try:
                skills_button = None
                
                if selector:
                    skills_button = await page.query_selector(selector)
                
                if not skills_button and href:
                    skills_button = await page.query_selector(f'a[href="{href}"]')
                
                if not skills_button:
                    skills_button = await page.query_selector('a[data-field="education_skill_associations"]')
                
                if not skills_button and href:
                    href_key = href.split('/')[-1].split('?')[0] if '/' in href else href
                    if href_key:
                        skills_button = await page.query_selector(f'a[href*="{href_key}"]')
                
                if not skills_button:
                    skills_button = await page.query_selector('a[href*="skill-associations"]')
                
                if skills_button:
                    await skills_button.scroll_into_view_if_needed()
                    await asyncio.sleep(0.5)
                    await skills_button.click()
                    await asyncio.sleep(2)
                    
                    # Extract skills from modal (reuse experience method)
                    skills = await self._extract_skills_from_modal(page)
                    
                    # Close modal
                    try:
                        await page.keyboard.press('Escape')
                        await asyncio.sleep(0.5)
                    except:
                        close_btn = await page.query_selector('.artdeco-modal__dismiss, button[aria-label*="Close"], button[aria-label*="Dismiss"]')
                        if close_btn:
                            await close_btn.click()
                            await asyncio.sleep(0.5)
                    
            except Exception as e:
                logger.debug(f"Error clicking education skills button: {e}")
            
            # Method 2: If clicking failed, try navigating to URL
            if not skills and href:
                try:
                    full_url = href if href.startswith('http') else f"https://www.linkedin.com{href}"
                    original_url = page.url
                    
                    await page.goto(full_url, wait_until='domcontentloaded', timeout=20000)
                    await asyncio.sleep(2)
                    
                    skills = await self._extract_skills_from_modal(page)
                    
                    # Navigate back
                    await page.goto(original_url, wait_until='domcontentloaded', timeout=15000)
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.debug(f"Error navigating to education skills page: {e}")
            
            return skills
            
        except Exception as e:
            logger.error(f"Error extracting education skills: {e}")
            return skills
    
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
                       'birthday', 'born', 'whatsapp', 'telegram', 'skype', 'youtube', 'https', 'http', '@', '.com', '.org', '.net',
                       'connected']  # Added 'connected' to capture Connected date section
            
            contact_lines = []
            for line in lines:
                # Check if line contains contact-related keywords or looks like a domain/email
                if any(kw in line.lower() for kw in keywords):
                    contact_lines.append(line)
                elif re.search(r'[\w\-]+\.[\w]{2,}', line):  # Looks like domain/URL
                    contact_lines.append(line)
                elif re.search(r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}', line):  # Phone pattern
                    contact_lines.append(line)
                elif re.search(r'[A-Za-z]+\s+\d{1,2}(?:,\s*\d{4})?', line):  # Date pattern (like April 8 or Nov 28, 2025)
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
                        timeout=20000,
                        max_retries=1
                    )
                    
                    if response:
                        await self.human_behavior.random_delay(0.5, 1)
                        
                        # Extract STRUCTURED contact info from overlay
                        page_html = await self.browser.page.content()

                        # --- Extract full website URLs from href attributes in contact info section using Playwright ---
                        # This extracts the FULL URL from the href attribute (like right-click copy link)
                        href_websites: list[str] = []
                        try:
                            from urllib.parse import urlparse

                            # MOST RELIABLE METHOD: Direct extraction from Website section
                            # LinkedIn structure: section.pv-contact-info__contact-type > h3 "Website" > ul > li > a[href]
                            website_links = await self.browser.page.evaluate("""
                                () => {
                                    const websites = [];
                                    
                                    // Method 1: Find all sections and look for Website header
                                    const sections = document.querySelectorAll('section');
                                    for (const section of sections) {
                                        const header = section.querySelector('h3');
                                        if (header && header.textContent && header.textContent.trim().toLowerCase() === 'website') {
                                            // Found Website section! Get all anchor tags
                                            const links = section.querySelectorAll('a');
                                            for (const link of links) {
                                                // link.href gives the FULL resolved URL (not display text)
                                                const href = link.href;
                                                if (href && href.startsWith('http') && !href.includes('linkedin.com')) {
                                                    websites.push(href);
                                                }
                                            }
                                        }
                                    }
                                    
                                    // Method 2: Direct class selector for contact links
                                    if (websites.length === 0) {
                                        const contactLinks = document.querySelectorAll('a.pv-contact-info__contact-link');
                                        for (const link of contactLinks) {
                                            const href = link.href;
                                            if (href && href.startsWith('http') && !href.includes('linkedin.com')) {
                                                // Check if this is in a Website section by looking at parent text
                                                let parent = link.parentElement;
                                                for (let i = 0; i < 5 && parent; i++) {
                                                    const parentText = parent.textContent || '';
                                                    if (parentText.toLowerCase().includes('website')) {
                                                        websites.push(href);
                                                        break;
                                                    }
                                                    parent = parent.parentElement;
                                                }
                                            }
                                        }
                                    }
                                    
                                    // Method 3: Look for any link after "Website" text
                                    if (websites.length === 0) {
                                        const allText = document.body.innerText;
                                        const websiteIndex = allText.toLowerCase().indexOf('website');
                                        if (websiteIndex !== -1) {
                                            // Website section exists, find links in contact modal
                                            const modal = document.querySelector('[role="dialog"]');
                                            if (modal) {
                                                const allLinks = modal.querySelectorAll('a[href^="http"]');
                                                for (const link of allLinks) {
                                                    const href = link.href;
                                                    // Skip social/linkedin links
                                                    if (href && 
                                                        !href.includes('linkedin.com') && 
                                                        !href.includes('github.com') &&
                                                        !href.includes('twitter.com') &&
                                                        !href.includes('facebook.com')) {
                                                        websites.push(href);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    
                                    // Deduplicate
                                    return [...new Set(websites)];
                                }
                            """)
                            
                            # Ensure website_links is a list
                            if not isinstance(website_links, list):
                                website_links = []
                            
                            # Log what was found by JavaScript extraction
                            if website_links:
                                logger.info(f"[WEBSITE] Extracted {len(website_links)} website URLs: {website_links}")
                                # Directly use what we found - these are from the Website section
                                href_websites = website_links.copy()
                            else:
                                logger.debug("[WEBSITE] No websites found in contact info")
                            
                        except Exception as e:
                            logger.debug(f"Error extracting websites from contact overlay: {e}")
                            href_websites = []
                        # --- END href extraction ---
                        
                        # --- Extract Connected date directly from HTML using JavaScript ---
                        connected_date = None
                        try:
                            connected_date = await self.browser.page.evaluate("""
                                () => {
                                    // Find Connected section
                                    const sections = document.querySelectorAll('section.pv-contact-info__contact-type');
                                    for (const section of sections) {
                                        const header = section.querySelector('h3');
                                        if (header && header.textContent && header.textContent.trim().toLowerCase() === 'connected') {
                                            // Found Connected section! Get the date
                                            const dateSpan = section.querySelector('span');
                                            if (dateSpan) {
                                                const dateText = dateSpan.textContent.trim();
                                                // Match date pattern like "Nov 28, 2025" or "November 28, 2025"
                                                const dateMatch = dateText.match(/([A-Za-z]+\\s+\\d{1,2},?\\s+\\d{4})/);
                                                if (dateMatch) {
                                                    return dateMatch[1].trim();
                                                }
                                                // Also try without year
                                                const dateMatchNoYear = dateText.match(/([A-Za-z]+\\s+\\d{1,2})/);
                                                if (dateMatchNoYear) {
                                                    return dateMatchNoYear[1].trim();
                                                }
                                                // Return raw text if it looks like a date
                                                if (dateText && dateText.length < 50 && /[A-Za-z]/.test(dateText)) {
                                                    return dateText;
                                                }
                                            }
                                        }
                                    }
                                    return null;
                                }
                            """)
                            
                            if connected_date:
                                logger.info(f"[CONNECTED] Extracted connected date: {connected_date}")
                        except Exception as e:
                            logger.debug(f"Error extracting connected date from contact overlay: {e}")
                            connected_date = None
                        # --- END Connected date extraction ---

                        contact_text = await self._parse_overlay_html(page_html)
                        
                        if contact_text and len(contact_text) > 50:
                            logger.debug(f"Got contact info from overlay: {len(contact_text)} chars")
                            
                            # Parse and return
                            contact_info = self.data_extractor.parse_contact_info(contact_text)

                            # DIRECTLY use href-extracted websites - NO FILTERING!
                            # The JavaScript already filters out linkedin.com links
                            # href_websites contains full URLs like "http://www.johndinsmore.com"
                            if href_websites:
                                contact_info["websites"] = href_websites
                                logger.info(f"[WEBSITE] Final websites: {contact_info['websites']}")
                            else:
                                # Only set N/A if nothing was extracted
                                if not contact_info.get("websites") or contact_info.get("websites") == ["N/A"]:
                                    contact_info["websites"] = ["N/A"]
                            
                            # DIRECTLY use JavaScript-extracted connected date
                            if connected_date:
                                contact_info["connected"] = [connected_date]
                                logger.info(f"[CONNECTED] Final connected date: {contact_info['connected']}")
                            # If JavaScript extraction didn't find it, parse_contact_info fallback will handle it
                            
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
