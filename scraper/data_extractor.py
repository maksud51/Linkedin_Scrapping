"""
Advanced JavaScript-Based Data Extractor
- Extracts data using JavaScript evaluation for reliable parsing
- Text-based extraction resistant to HTML changes
- Dynamic section detection with intelligent fallbacks
- Completeness scoring (0-100%)
"""

import re
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
from playwright.async_api import Page
import logging

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extract LinkedIn profile data using JavaScript evaluation + text parsing"""
    
    def __init__(self):
        self.extracted_data = {}
        self.scrape_agent = None  # Will be set by scrape_agent when needed
    
    async def extract_complete_profile(self, page: Page, profile_url: str) -> Optional[Dict]:
        """Extract complete profile using JavaScript evaluation"""
        try:
            logger.info(f"Extracting profile data from {profile_url}")
            
            # Get all page text via JavaScript (most reliable method)
            all_text = await self._extract_all_with_js(page)
            
            if not all_text:
                logger.warning("Could not extract page content")
                return None
            
            profile_data = {
                'profile_url': profile_url,
                'scraped_at': datetime.now().isoformat(),
                'extraction_method': 'javascript-text-based'
            }
            
            # Extract basic info - use JavaScript first, then text fallbacks
            profile_data['name'] = await self._extract_name(page, all_text)
            profile_data['headline'] = await self._extract_headline(page, all_text)
            profile_data['location'] = await self._extract_location(page, all_text)
            profile_data['about'] = await self._extract_about(page, all_text)
            
            # Extract contact info (if scrape_agent is available)
            if self.scrape_agent:
                contact_info = await self.scrape_agent._extract_contact_info()
                if contact_info:
                    profile_data['contact_info'] = contact_info
                    logger.debug("Extracted contact info via scrape_agent")
            
            # Extract sections using text parsing
            profile_data['experience'] = await self._extract_experience(page, all_text)
            profile_data['education'] = await self._extract_education(page, all_text)
            profile_data['skills'] = await self._extract_skills(page, all_text)
            profile_data['certifications'] = await self._extract_certifications(page, all_text)
            profile_data['projects'] = await self._extract_projects(page, all_text)
            profile_data['languages'] = await self._extract_languages(page, all_text)
            profile_data['recommendations'] = await self._extract_recommendations(page, all_text)
            
            # Calculate completeness score
            profile_data['completeness'] = self._calculate_completeness(profile_data)
            
            logger.info(f"Profile extraction completed: {profile_data.get('name', 'Unknown')} ({profile_data['completeness']}% complete)")
            return profile_data
            
        except Exception as e:
            logger.error(f"Profile extraction failed: {e}")
            return None
    
    async def _extract_all_with_js(self, page: Page) -> Optional[str]:
        """Extract all page text using JavaScript - most reliable method"""
        try:
            all_text = await page.evaluate("""
                () => {
                    // Get all visible text from page
                    const allText = document.body.innerText;
                    return allText || null;
                }
            """)
            return all_text
        except Exception as e:
            logger.warning(f"Error extracting with JavaScript: {e}")
            return None
    
    async def _extract_name(self, page: Page, all_text: str) -> Optional[str]:
        """Extract name using multiple JavaScript methods"""
        try:
            # Skip JavaScript - it's picking up navigation text
            # Go straight to fallback text extraction which is more reliable
            name = await self._extract_name_fallback(all_text)
            if name:
                return name
            
            # If fallback didn't work, try this JavaScript as last resort
            # Look for the LinkedIn profile header structure (main profile section)
            name = await page.evaluate("""
                () => {
                    // The profile name is typically the first h1 in the main profile area
                    // LinkedIn puts the name in a specific section - look for visible text
                    const allH1s = document.querySelectorAll('h1');
                    const navigationText = ['Skip to main content', 'For Business', 'Sign in', 'Join now', 
                                          'Search', 'Home', 'My Network', 'Messaging', 'Notifications',
                                          'Jobs', 'Learning', 'Show all', 'More', 'Upgrade'];
                    
                    // Find the first h1 that's NOT navigation text
                    for (let h1 of allH1s) {
                        const text = h1.innerText.trim();
                        const isNavigation = navigationText.some(nav => nav.toLowerCase() === text.toLowerCase());
                        
                        if (text && !isNavigation && text.length > 2 && text.length < 150) {
                            // Additional validation - name should have 1-5 words and be mostly letters
                            const words = text.split(/\\s+/).filter(w => w);
                            if (words.length >= 1 && words.length <= 5) {
                                // Check if it looks like a real name (has letters)
                                if (/[a-zA-Z]/.test(text)) {
                                    return text;
                                }
                            }
                        }
                    }
                    return null;
                }
            """)
            
            if name and len(name) > 2 and len(name) < 150:
                return name.strip()
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting name with JS: {e}")
            return await self._extract_name_fallback(all_text)
    
    async def _extract_name_fallback(self, all_text: str) -> Optional[str]:
        """Fallback name extraction from text - try multiple strategies"""
        try:
            # Split by common text separators since about is often one long line
            # Replace common separators with newlines to split better
            text = all_text.replace('•', '\n').replace('·', '\n').replace('|', '\n')
            text = text.replace('Activity', '\nActivity\n')  # Mark activity section
            lines = text.split('\n')
            
            # Skip navigation text, find real name
            skip_words = ['skip', 'main', 'content', 'button', 'http', 'follow', 'message', 'more', 
                         'click', 'my network', 'network', 'show all', 'for business', 'sign in', 'join now',
                         'try premium', 'get up to', 'upgrade', 'followers', 'following', 'posts', 'comments']
            
            # Strategy 1: Look for name in first 30 lines (profile header area)
            for line in lines[:30]:
                line = line.strip()
                if line and len(line) > 2 and len(line) < 150:
                    # Check if line looks like a name (not navigation text)
                    if not any(skip in line.lower() for skip in skip_words):
                        # Name typically has 2-5 words, starts with capital
                        words = line.split()
                        if 2 <= len(words) <= 5 and len(words) >= 2 and line[0].isupper():
                            return line
            
            # Strategy 2: Extract from activity patterns like "X commented on a post", "X reposted", etc
            # Look for these activity verbs
            activity_patterns = ['commented on a post', ' reposted ', ' posted ', ' liked ']
            
            for pattern in activity_patterns:
                if pattern in all_text:
                    # Find the first occurrence
                    idx = all_text.find(pattern)
                    if idx > 0:
                        # Get text before the pattern
                        before_text = all_text[:idx].strip()
                        # Split into words
                        parts = re.split(r'[\s•·\-]+', before_text)
                        # Get the last non-empty parts
                        parts = [p.strip() for p in parts if p.strip()]
                        if parts:
                            # Usually the name is the last 2-4 words (prefer 3-4 for full names)
                            for num_words in [4, 3, 2]:
                                if len(parts) >= num_words:
                                    name_part = ' '.join(parts[-num_words:])
                                    name_part = name_part.strip()
                                    if name_part and len(name_part) > 2 and len(name_part) < 100:
                                        # Check if first char is uppercase and not in skip list
                                        if name_part[0].isupper() and not any(skip in name_part.lower() for skip in skip_words):
                                            # Should have at least one space (multiple words)
                                            if ' ' in name_part:
                                                return name_part
            
            return None
        except Exception as e:
            logger.debug(f"Error in fallback name extraction: {e}")
            return None
    
    async def _extract_headline(self, page: Page, all_text: str) -> Optional[str]:
        """Extract headline (job title/skills) using JavaScript"""
        try:
            # JavaScript method - look for the specific headline structure
            headline = await page.evaluate("""
                () => {
                    // Look for the text-body-medium div right after name (contains headline)
                    const headlineDiv = document.querySelector('.text-body-medium[data-generated-suggestion-target*="profileActionDelegate"]');
                    if (headlineDiv) {
                        const text = headlineDiv.innerText.trim();
                        if (text && text.length > 3 && text.length < 500 && 
                            !text.includes('Get up to') && !text.includes('InMail') && !text.includes('message')) {
                            return text;
                        }
                    }
                    
                    // Alternative: Look for any div with medium text size containing pipe or skills
                    const mediumTexts = document.querySelectorAll('.text-body-medium, [class*="headline"]');
                    for (let elem of mediumTexts) {
                        const text = elem.innerText.trim();
                        if (text && (text.includes('|') || text.includes('Machine') || text.includes('Engineer') || 
                                    text.includes('Developer') || text.includes('Robotics')) && 
                            text.length > 3 && text.length < 500 &&
                            !text.includes('Get up to') && !text.includes('InMail')) {
                            return text;
                        }
                    }
                    
                    return null;
                }
            """)
            
            if headline and len(headline) > 3 and len(headline) < 500 and 'get up to' not in headline.lower():
                return headline.strip()
            
            # Text fallback - look for headlines after name
            return await self._extract_headline_fallback(all_text)
            
        except Exception as e:
            logger.debug(f"Error extracting headline: {e}")
            return await self._extract_headline_fallback(all_text)
    
    async def _extract_headline_fallback(self, all_text: str) -> Optional[str]:
        """Fallback headline extraction from page content - look for | or engineering keywords"""
        try:
            lines = all_text.split('\n')
            # Headline usually appears in first 40 lines and contains job-related keywords or pipes
            headline_keywords = ['engineer', 'developer', 'manager', 'lead', 'specialist', 'architect',
                               'robotics', 'learning', 'ai', 'ml', 'python', 'founder', 'ceo', 'researcher', 'scientist']
            
            for line in lines[2:50]:
                line = line.strip()
                if line and len(line) > 5 and len(line) < 500:
                    # Check for pipe separator (common in LinkedIn headlines)
                    if '|' in line:
                        return line
                    # Or check for keywords
                    if any(kw in line.lower() for kw in headline_keywords):
                        # Skip if it contains too much text (probably from about section)
                        if len(line) < 200 and line.count(' ') < 30:
                            return line
            return None
        except:
            return None
    
    async def _extract_location(self, page: Page, all_text: str) -> Optional[str]:
        """Extract location using JavaScript and text parsing"""
        try:
            # JavaScript method - look for location text in specific patterns
            location = await page.evaluate("""
                () => {
                    // Look for text-body-small containing location info
                    const locationSpans = document.querySelectorAll('.text-body-small');
                    for (let span of locationSpans) {
                        const text = span.innerText.trim();
                        // Location usually has comma and specific patterns
                        if (text && text.includes(',') && text.length > 3 && text.length < 150 && 
                            !text.includes('http') && !text.includes('Follow') && !text.includes('Message')) {
                            // Check if it looks like a location (has words like "Area", city patterns, country)
                            if (text.match(/[A-Za-z]+,\\s*[A-Za-z]+/) || 
                                text.includes('Area') || text.includes('Remote') || text.includes('Based')) {
                                return text;
                            }
                        }
                    }
                    
                    return null;
                }
            """)
            
            if location:
                return location.strip()
            
            # Text fallback
            return await self._extract_location_fallback(all_text)
            
        except Exception as e:
            logger.debug(f"Error extracting location: {e}")
            return await self._extract_location_fallback(all_text)
    
    async def _extract_location_fallback(self, all_text: str) -> Optional[str]:
        """Fallback location extraction from text"""
        try:
            lines = all_text.split('\n')
            # Location typically appears early in profile, has comma, and follows education/work info
            for i, line in enumerate(lines[:50]):
                line = line.strip()
                # Look for pattern: City, Country or City, State, Country
                if line and ',' in line and len(line) > 3 and len(line) < 150:
                    # Check for common location indicators
                    if not any(x in line.lower() for x in ['http', 'button', 'follow', 'message', 'skill', 'education', 'experience']):
                        # Simple heuristic: if has 2+ parts separated by comma with alphabetic chars
                        parts = line.split(',')
                        if len(parts) >= 2 and all(len(p.strip()) > 0 for p in parts):
                            return line
            return None
        except:
            return None
    
    async def _extract_about(self, page: Page, all_text: str) -> Optional[str]:
        """Extract About section"""
        try:
            lines = all_text.split('\n')
            about_started = False
            about_text = []
            
            for i, line in enumerate(lines):
                if 'About' in line:
                    about_started = True
                    continue
                
                if about_started:
                    # Stop at next section
                    if any(section in line for section in ['Experience', 'Education', 'Skills', 'Licenses']):
                        break
                    
                    line = line.strip()
                    if line and not any(x in line for x in ['http', 'button', 'Follow']):
                        about_text.append(line)
            
            return ' '.join(about_text).strip() if about_text else None
            
        except Exception as e:
            logger.debug(f"Error extracting about: {e}")
            return None
    
    async def _extract_experience(self, page: Page, all_text: str) -> List[Dict]:
        """Extract experience entries from text (robust section detection)"""
        experiences = []
        try:
            lines = all_text.split('\n')
            exp_started = False
            current_exp = None
            section_headers = ['experience', 'work experience', 'professional experience']
            stop_sections = ['education', 'skills', 'licenses', 'certifications', 'projects', 'languages', 'recommendations']
            for i, line in enumerate(lines):
                line_clean = line.strip().lower()
                # Start of experience section (case-insensitive, alternate names)
                if any(h in line_clean for h in section_headers) and not exp_started:
                    exp_started = True
                    continue
                # Stop at next major section
                if exp_started and any(section in line_clean for section in stop_sections):
                    if current_exp and current_exp.get('title'):
                        experiences.append(current_exp)
                    break
                if exp_started and line:
                    # New job entry - starts with job title (no special characters)
                    if not any(c in line.lower() for c in ['http', 'follow', 'endorse', 'button']) and len(line) > 5:
                        if current_exp and current_exp.get('title'):
                            experiences.append(current_exp)
                        current_exp = {'title': line}
                    elif current_exp:
                        if 'company' not in current_exp and (any(x in line for x in ['Inc', 'Ltd', ',']) or len(line) > 20):
                            current_exp['company'] = line
                        elif 'duration' not in current_exp and any(c.isdigit() for c in line):
                            current_exp['duration'] = line
                        elif 'description' not in current_exp and len(line) > 10:
                            current_exp['description'] = line
            if current_exp and current_exp.get('title'):
                experiences.append(current_exp)
            logger.info(f"Extracted {len(experiences)} experience entries")
            return experiences
        except Exception as e:
            logger.warning(f"Error extracting experience: {e}")
            return experiences
    
    async def _extract_education(self, page: Page, all_text: str) -> List[Dict]:
        """Extract education entries (robust section detection)"""
        education = []
        try:
            lines = all_text.split('\n')
            edu_started = False
            current_edu = None
            section_headers = ['education', 'academic background', 'studies']
            stop_sections = ['skills', 'licenses', 'projects', 'languages', 'recommendations', 'experience']
            for line in lines:
                line_clean = line.strip().lower()
                if any(h in line_clean for h in section_headers) and not edu_started:
                    edu_started = True
                    continue
                if edu_started and any(section in line_clean for section in stop_sections):
                    if current_edu and current_edu.get('school'):
                        education.append(current_edu)
                    break
                if edu_started and line:
                    if not any(c in line.lower() for c in ['http', 'follow', 'button']) and len(line) > 3:
                        if 'school' not in current_edu if current_edu else True:
                            if current_edu and current_edu.get('school'):
                                education.append(current_edu)
                            current_edu = {'school': line}
                        elif current_edu:
                            if 'degree' not in current_edu:
                                current_edu['degree'] = line
                            elif 'duration' not in current_edu and any(c.isdigit() for c in line):
                                current_edu['duration'] = line
            if current_edu and current_edu.get('school'):
                education.append(current_edu)
            logger.info(f"Extracted {len(education)} education entries")
            return education
        except Exception as e:
            logger.warning(f"Error extracting education: {e}")
            return education
    
    async def _extract_skills(self, page: Page, all_text: str) -> List[str]:
        """Extract skills list (robust section detection)"""
        skills = []
        try:
            lines = all_text.split('\n')
            skill_started = False
            section_headers = ['skills', 'core skills', 'competencies']
            stop_sections = ['licenses', 'projects', 'languages', 'certifications', 'recommendations', 'education', 'experience']
            for line in lines:
                line_clean = line.strip().lower()
                if any(h in line_clean for h in section_headers):
                    skill_started = True
                    continue
                if skill_started:
                    if any(section in line_clean for section in stop_sections):
                        break
                    if line and not any(c in line.lower() for c in ['http', 'follow', 'endorse', 'button']):
                        skill_clean = re.sub(r'\d+\s*(endorsements?)?', '', line, flags=re.IGNORECASE).strip()
                        if skill_clean and len(skill_clean) > 1 and len(skill_clean) < 100:
                            skills.append(skill_clean)
            skills = list(dict.fromkeys(skills))
            logger.info(f"Extracted {len(skills)} skills")
        except Exception as e:
            logger.warning(f"Error extracting skills: {e}")
        return skills
    
    async def _extract_certifications(self, page: Page, all_text: str) -> List[Dict]:
        """Extract certifications"""
        certs = []
        try:
            lines = all_text.split('\n')
            cert_started = False
            current_cert = None
            
            for line in lines:
                line = line.strip()
                
                if 'Licenses' in line or 'Certifications' in line:
                    cert_started = True
                    continue
                
                if cert_started and any(section in line for section in ['Projects', 'Languages', 'Skills']):
                    if current_cert and current_cert.get('name'):
                        certs.append(current_cert)
                    break
                
                if cert_started and line:
                    if 'name' not in (current_cert or {}):
                        current_cert = {'name': line}
                    elif current_cert:
                        if 'issuer' not in current_cert:
                            current_cert['issuer'] = line
                        elif 'date' not in current_cert and any(c.isdigit() for c in line):
                            current_cert['date'] = line
            
            if current_cert and current_cert.get('name'):
                certs.append(current_cert)
            
            logger.info(f"Extracted {len(certs)} certifications")
            return certs
            
        except Exception as e:
            logger.warning(f"Error extracting certifications: {e}")
            return certs
    
    async def _extract_projects(self, page: Page, all_text: str) -> List[Dict]:
        """Extract projects"""
        projects = []
        try:
            lines = all_text.split('\n')
            proj_started = False
            current_proj = None
            
            for line in lines:
                line = line.strip()
                
                if 'Projects' in line:
                    proj_started = True
                    continue
                
                if proj_started and any(section in line for section in ['Languages', 'Recommendations', 'Skills']):
                    if current_proj and current_proj.get('name'):
                        projects.append(current_proj)
                    break
                
                if proj_started and line and len(line) > 3:
                    if 'name' not in (current_proj or {}):
                        current_proj = {'name': line}
                    elif current_proj and 'description' not in current_proj:
                        current_proj['description'] = line
            
            if current_proj and current_proj.get('name'):
                projects.append(current_proj)
            
            return projects
            
        except Exception as e:
            logger.warning(f"Error extracting projects: {e}")
            return projects
    
    async def _extract_languages(self, page: Page, all_text: str) -> List[str]:
        """Extract languages"""
        languages = []
        try:
            lines = all_text.split('\n')
            lang_started = False
            
            for line in lines:
                line = line.strip()
                
                if line == 'Languages':
                    lang_started = True
                    continue
                
                if lang_started:
                    if any(section in line for section in ['Recommendations', 'Projects', 'Skills', 'Education']):
                        break
                    
                    if line and not any(c in line for c in ['http', 'Follow', 'button']):
                        if len(line) > 1 and len(line) < 50:
                            languages.append(line)
            
            languages = list(dict.fromkeys(languages))  # Remove duplicates
            
        except Exception as e:
            logger.warning(f"Error extracting languages: {e}")
        
        return languages
    
    async def _extract_recommendations(self, page: Page, all_text: str) -> List[Dict]:
        """Extract recommendations"""
        recommendations = []
        try:
            # Try to extract from page
            rec_data = await page.evaluate("""
                () => {
                    const recs = [];
                    const divs = document.querySelectorAll('[class*="recommendation"]');
                    
                    for (let div of divs) {
                        const text = div.innerText;
                        if (text && text.length > 20) {
                            recs.push({text: text.substring(0, 500)});
                        }
                    }
                    return recs;
                }
            """)
            
            recommendations = rec_data[:5] if rec_data else []
            
        except Exception as e:
            logger.debug(f"Error extracting recommendations: {e}")
        
        return recommendations
    
    async def _extract_contact_info_from_page(self, page: Page, all_text: str) -> Optional[Dict]:
        """Try to extract LinkedIn profile URL from the page (visible without modal)"""
        try:
            contact_info = {}
            
            # LinkedIn profile URL patterns (may or may not have https://)
            url_patterns = [
                r'https?://(?:www\.)?linkedin\.com/in/[\w\-]+',  # Full URL with https
                r'linkedin\.com/in/[\w\-]+',  # URL without https
            ]
            
            # Search in all_text first (most reliable as it's visible text)
            for pattern in url_patterns:
                linkedin_match = re.search(pattern, all_text, re.IGNORECASE)
                if linkedin_match:
                    url = linkedin_match.group()
                    # Ensure it has https:// prefix
                    if not url.startswith('http'):
                        url = 'https://www.' + url
                    contact_info['linkedin_url'] = url
                    logger.debug(f"Found LinkedIn URL in text: {contact_info['linkedin_url']}")
                    return contact_info
            
            # If not found in text, try in page HTML
            try:
                page_html = await page.content()
                for pattern in url_patterns:
                    linkedin_match = re.search(pattern, page_html, re.IGNORECASE)
                    if linkedin_match:
                        url = linkedin_match.group()
                        # Ensure it has https:// prefix
                        if not url.startswith('http'):
                            url = 'https://www.' + url
                        contact_info['linkedin_url'] = url
                        logger.debug(f"Found LinkedIn URL in HTML: {contact_info['linkedin_url']}")
                        return contact_info
            except:
                pass
            
            # Return None if nothing found
            logger.debug("LinkedIn URL not found on page")
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting contact info from page: {e}")
            return None
    
    def parse_contact_info(self, contact_text: str) -> Dict[str, any]:
        """Parse comprehensive contact info from modal/overlay text - extracts ALL contact types"""
        contact_info = {}
        try:
            if not contact_text:
                return contact_info
            
            # Clean up the raw text by extracting only the relevant section
            # Find the "Contact info" section and extract content between it and the next major section
            lines = contact_text.split('\n')
            
            # Find the contact info section
            contact_section_start = -1
            for i, line in enumerate(lines):
                if 'contact info' in line.lower():
                    contact_section_start = i
                    break
            
            # If we found contact section, extract content after it until we hit navigation or end
            if contact_section_start >= 0:
                contact_lines = []
                for i in range(contact_section_start + 1, len(lines)):
                    line = lines[i]
                    # Stop at common section markers
                    if any(marker in line.lower() for marker in ['about', 'experience', 'education', 'skills', 'recommendations', 'causes']):
                        break
                    # Add non-empty lines
                    if line.strip() and len(line.strip()) > 1:
                        contact_lines.append(line.strip())
                
                # Reconstruct cleaned contact text
                if contact_lines:
                    contact_text = '\n'.join(contact_lines)
            
            # Store raw text for reference (cleaned version)
            contact_info['raw_text'] = contact_text
            
            # ========== EMAIL EXTRACTION ==========
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails = re.findall(email_pattern, contact_text)
            contact_info['emails'] = emails if emails else ['N/A']
            
            # ========== PHONE EXTRACTION (Multiple) ==========
            phones = []
            phone_patterns = [
                r'(?:phone|tel|mobile)[:\s]+(\+?[\d\s.\-()]{8,})',  # Labeled phones
                r'\+[\d]{1,3}[\d\s.\-()]{8,}',  # International format
                r'(?:^|\s)[\d]{3}[-.]?[\d]{3}[-.]?[\d]{4}(?:\s|$)',  # Standard format
                r'(?:^|\s)\([\d]{3}\)[\s]?[\d]{3}[-.][\d]{4}(?:\s|$)',  # (XXX) XXX-XXXX
            ]
            for pattern in phone_patterns:
                matches = re.findall(pattern, contact_text, re.IGNORECASE | re.MULTILINE)
                phones.extend(matches)
            contact_info['phones'] = list(set([p.strip() for p in phones])) if phones else ['N/A']
            
            # ========== LINKEDIN URL EXTRACTION (Multiple) ==========
            linkedin_urls = re.findall(
                r'(?:https?://)?(?:www\.)?linkedin\.com/in/[\w\-]+',
                contact_text,
                re.IGNORECASE
            )
            # Also extract just the username if preceded by linkedin.com/in/
            linkedin_usernames = re.findall(
                r'linkedin\.com/in/([\w\-]+)',
                contact_text,
                re.IGNORECASE
            )
            
            # Ensure https:// prefix for full URLs
            linkedin_urls = list(set([
                (u if u.startswith('http') else 'https://' + u) 
                for u in linkedin_urls
            ]))
            
            # Add usernames as full URLs if not already present
            for username in linkedin_usernames:
                full_url = f'https://linkedin.com/in/{username}'
                if full_url not in linkedin_urls:
                    linkedin_urls.append(full_url)
            
            contact_info['linkedin_urls'] = list(set(linkedin_urls)) if linkedin_urls else ['N/A']
            
            # ========== GITHUB EXTRACTION (Multiple) ==========
            github_urls = re.findall(
                r'https?://(?:www\.)?github\.com/[\w\-]+|github\.com/[\w\-]+',
                contact_text,
                re.IGNORECASE
            )
            github_urls = [
                (u if u.startswith('http') else 'https://' + u) 
                for u in github_urls
            ]
            contact_info['github_urls'] = list(set(github_urls)) if github_urls else ['N/A']
            
            # ========== WEBSITES EXTRACTION (Multiple) ==========
            all_urls = re.findall(r'https?://[^\s<>"\)]+', contact_text)
            websites = []
            for url in all_urls:
                # Exclude LinkedIn and GitHub URLs
                if not any(x in url.lower() for x in ['linkedin', 'github']):
                    # Clean up URL
                    url = url.rstrip('.,;:\'")')
                    if len(url) > 7:  # At least http://a.b
                        websites.append(url)
            
            # Also extract domain names without http:// (like cuet.ac.bd, maksudcse.wordpress.com)
            # More flexible pattern that captures common domain extensions
            domain_pattern = r'\b([a-zA-Z0-9][\w\-]{0,62}(?:\.[a-zA-Z0-9][\w\-]{0,62})*\.(?:ac\.bd|com|org|net|edu|io|co|bd|wordpress\.com|github\.io|dev|app|in|us|uk|au|ca|de|fr|jp|cn|ru|in|tv|xyz|info|biz|name|me|cc))\b'
            domains = re.findall(domain_pattern, contact_text, re.IGNORECASE | re.MULTILINE)
            
            # Filter out known non-website / noise domains and duplicates
            filtered_domains = []
            for domain in domains:
                domain_lower = domain.lower()
                if any(x in domain_lower for x in [
                    'linkedin', 'github', 'facebook', 'twitter', 'instagram', 'youtube',
                    'corp', 'static.licdn.com', 'licdn.com',
                    'gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com', 'proton.me'
                ]):
                    continue
                filtered_domains.append(domain)
            
            websites.extend(filtered_domains)

            # Final cleanup for websites list (remove LICDN assets, email providers, etc.)
            cleaned_websites = []
            for url in websites:
                url_str = str(url).strip()
                lower = url_str.lower()
                if not url_str:
                    continue
                if any(x in lower for x in [
                    'static.licdn.com', 'media.licdn.com',
                    'linkedin.com/feed', 'mailto:', 'javascript:'
                ]):
                    continue
                if lower in ['gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com', 'proton.me']:
                    continue
                if url_str not in cleaned_websites:
                    cleaned_websites.append(url_str)

            contact_info['websites'] = cleaned_websites if cleaned_websites else ['N/A']
            
            # ========== TWITTER EXTRACTION ==========
            twitter_patterns = [
                r'(?:twitter\.com/|@)(?P<handle>[\w\-]{1,15})',
                r'https?://(?:www\.)?twitter\.com/[\w\-]+',
            ]
            twitter_handles = []
            for pattern in twitter_patterns:
                matches = re.findall(pattern, contact_text, re.IGNORECASE)
                twitter_handles.extend(matches)
            contact_info['twitter'] = list(set(twitter_handles)) if twitter_handles else ['N/A']
            
            # ========== INSTAGRAM EXTRACTION ==========
            instagram_patterns = [
                r'(?:instagram\.com/|instagram\s+handle\s*:\s*)(?P<handle>[\w\.]{1,30})',
                r'instagram\s*:\s*(?P<handle>[\w\.]+)',
            ]
            insta_handles = []
            for pattern in instagram_patterns:
                matches = re.findall(pattern, contact_text, re.IGNORECASE)
                insta_handles.extend(matches)
            contact_info['instagram'] = list(set(insta_handles)) if insta_handles else ['N/A']
            
            # ========== FACEBOOK EXTRACTION ==========
            facebook_patterns = [
                r'(?:facebook\.com/|facebook\s+:\s*)(?P<handle>[\w\.\-]+)',
                r'facebook\s*:\s*(?P<handle>[\w\.\-]+)',
            ]
            fb_handles = []
            for pattern in facebook_patterns:
                matches = re.findall(pattern, contact_text, re.IGNORECASE)
                fb_handles.extend(matches)
            contact_info['facebook'] = list(set(fb_handles)) if fb_handles else ['N/A']
            
            # ========== WHATSAPP EXTRACTION ==========
            whatsapp_patterns = [
                r'(?:whatsapp|wa\.me)[:\s/]+(\+?[\d\s.\-()]{8,})',
                r'whatsapp\s*:\s*(\+?[\d\s.\-()]+)',
            ]
            whatsapp_nums = []
            for pattern in whatsapp_patterns:
                matches = re.findall(pattern, contact_text, re.IGNORECASE)
                whatsapp_nums.extend(matches)
            contact_info['whatsapp'] = list(set([w.strip() for w in whatsapp_nums])) if whatsapp_nums else ['N/A']
            
            # ========== TELEGRAM EXTRACTION ==========
            telegram_patterns = [
                r'(?:telegram|t\.me)[:\s/]+(?P<handle>[\w\-]{5,})',
                r'telegram\s*:\s*(?P<handle>[\w\-]+)',
            ]
            tg_handles = []
            for pattern in telegram_patterns:
                matches = re.findall(pattern, contact_text, re.IGNORECASE)
                tg_handles.extend(matches)
            contact_info['telegram'] = list(set(tg_handles)) if tg_handles else ['N/A']
            
            # ========== BIRTHDAY EXTRACTION ==========
            birthday_patterns = [
                r'(?:birthday|born|dob|date of birth)[:\s]+([A-Za-z]+\s+\d{1,2})',  # Month Day (e.g., April 8)
                r'\b([A-Za-z]+\s+\d{1,2})\b(?=\s|$)',  # Month Day anywhere
                r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})',  # DD-MM-YYYY or MM/DD/YY
                r'([A-Za-z]+\s+\d{1,2},?\s+\d{4})',  # Month Day, Year
            ]
            birthdays = []
            for pattern in birthday_patterns:
                matches = re.findall(pattern, contact_text, re.IGNORECASE | re.MULTILINE)
                birthdays.extend(matches)
            
            # Filter to valid months to avoid false positives
            valid_months = ['january', 'february', 'march', 'april', 'may', 'june', 
                          'july', 'august', 'september', 'october', 'november', 'december',
                          'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
            birthdays = [b for b in birthdays if any(month in b.lower() for month in valid_months)]
            contact_info['birthday'] = list(set([b.strip() for b in birthdays])) if birthdays else ['N/A']
            
            # ========== SKYPE EXTRACTION ==========
            skype_patterns = [
                r'(?:skype)[:\s]+(?P<handle>[\w\-\.]+)',
                r'skype\s*:\s*(?P<handle>[\w\-\.]+)',
            ]
            skype_handles = []
            for pattern in skype_patterns:
                matches = re.findall(pattern, contact_text, re.IGNORECASE)
                skype_handles.extend(matches)
            contact_info['skype'] = list(set(skype_handles)) if skype_handles else ['N/A']
            
            # ========== YOUTUBE EXTRACTION ==========
            youtube_urls = re.findall(
                r'https?://(?:www\.)?youtube\.com/(?:channel/|c/|user/)?[\w\-]+',
                contact_text,
                re.IGNORECASE
            )
            contact_info['youtube'] = list(set(youtube_urls)) if youtube_urls else ['N/A']
            
            # ========== TWITTER PROFILE EXTRACTION ==========
            twitter_urls = re.findall(
                r'https?://(?:www\.)?twitter\.com/[\w\-]+',
                contact_text,
                re.IGNORECASE
            )
            contact_info['twitter_url'] = list(set(twitter_urls)) if twitter_urls else ['N/A']
            
            # ========== LINKEDIN PROFILE URL (Primary) ==========
            if contact_info['linkedin_urls'][0] != 'N/A':
                contact_info['linkedin_url'] = contact_info['linkedin_urls'][0]
            else:
                contact_info['linkedin_url'] = 'N/A'
            
            logger.debug(f"Parsed comprehensive contact info with {len(contact_info)} fields")
            return contact_info
            
        except Exception as e:
            logger.debug(f"Error parsing contact info: {e}")
            return {'raw_text': contact_text, 'error': str(e)} if contact_text else {}
    
    def _calculate_completeness(self, profile_data: Dict) -> int:
        """Calculate profile completeness score (0-100%)"""
        try:
            fields = [
                profile_data.get('name'),
                profile_data.get('headline'),
                profile_data.get('location'),
                profile_data.get('about'),
                profile_data.get('experience'),
                profile_data.get('education'),
                profile_data.get('skills'),
            ]
            
            # Count non-empty fields
            filled = sum(1 for field in fields if field)
            completeness = (filled / len(fields)) * 100
            
            return int(completeness)
            
        except Exception as e:
            logger.debug(f"Error calculating completeness: {e}")
            return 0

