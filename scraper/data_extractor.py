"""
Advanced JavaScript-Based Data Extractor
- Extracts data using JavaScript evaluation for reliable parsing
- Text-based extraction resistant to HTML changes
- Dynamic section detection with intelligent fallbacks
- Completeness scoring (0-100%)
"""

import re
import json
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime
from urllib.parse import urlparse
from playwright.async_api import Page
import logging

logger = logging.getLogger(__name__)


class DataExtractor:
    """Extract LinkedIn profile data using JavaScript evaluation + text parsing"""
    
    # Shared domain filters for website extraction - COMPREHENSIVE LIST
    GENERIC_DOMAINS = [
        # Email providers (NEVER personal websites)
        "gmail.com", "mail.google.com", "outlook.com", "hotmail.com",
        "live.com", "yahoo.com", "ymail.com", "proton.me", "icloud.com",
        "aol.com", "zoho.com", "mail.com", "gmx.com",
        # Job sites (common for many people)
        "bdjobs.com", "jobs.com", "indeed.com", "monster.com", "careerbuilder.com",
        "glassdoor.com", "ziprecruiter.com", "naukri.com", "jobstreet.com",
        # Social media (handled separately)
        "facebook.com", "twitter.com", "instagram.com", "youtube.com",
        "tiktok.com", "snapchat.com", "pinterest.com", "reddit.com",
        "linkedin.com", "x.com",
        # Academic root domains (not personal websites - these are institutions)
        "diu.edu.bd", "edu.bd", "ac.bd", "edu.pk", "ac.uk", "edu.in",
        # Generic corporate/tech domains
        "github.com", "google.com", "microsoft.com", "apple.com",
        "amazon.com", "netflix.com", "spotify.com",
        # File hosting (not personal websites)
        "drive.google.com", "dropbox.com", "onedrive.com", "box.com",
        # LinkedIn assets
        "media.licdn.com", "static.licdn.com", "licdn.com",
        # Cloud services
        "aws.amazon.com", "azure.microsoft.com", "cloud.google.com",
        # News/Content sites
        "medium.com", "dev.to", "hashnode.dev",
        # Generic TLDs that are not personal
        "example.com", "test.com",
    ]
    
    # Pattern-based generic domain suffixes to filter out
    GENERIC_DOMAIN_SUFFIXES = [
        ".edu", ".edu.bd", ".edu.pk", ".edu.in", ".edu.au",
        ".ac.bd", ".ac.uk", ".ac.in", ".ac.jp",
        ".gov", ".gov.bd", ".gov.in", ".gov.uk",
        ".mil", ".org",  # Note: some .org might be personal but rare
    ]

    PERSONAL_PLATFORMS = [
        "sites.google.com", "github.io", "wordpress.com",
        "wix.com", "squarespace.com", "weebly.com", "carrd.co",
        "notion.site", "notion.so", "bio.link", "linktr.ee",
        "about.me", "behance.net", "dribbble.com", "portfolio.com",
        "myportfolio.com", "webflow.io", "vercel.app", "netlify.app",
    ]
    
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
            
            # ========== EXTRACT ADDITIONAL EMAILS FROM ALL SECTIONS ==========
            # This extracts emails from about, headline, experience, posts, projects etc.
            # Contact info emails are preserved - this ADDS to them
            additional_emails = self._extract_emails_from_all_sections(profile_data, all_text)
            
            # Merge with contact info emails (contact info emails take priority)
            if 'contact_info' in profile_data and profile_data['contact_info']:
                contact_emails = profile_data['contact_info'].get('emails', [])
                if contact_emails and contact_emails != ['N/A']:
                    # Start with contact info emails
                    all_emails = list(contact_emails)
                    # Add additional emails that aren't already present
                    for email in additional_emails:
                        if email.lower() not in [e.lower() for e in all_emails]:
                            all_emails.append(email)
                    profile_data['contact_info']['emails'] = all_emails if all_emails else ['N/A']
                elif additional_emails:
                    # No contact info emails, use additional ones
                    profile_data['contact_info']['emails'] = additional_emails
            elif additional_emails:
                # No contact info at all, create minimal contact_info with emails
                if 'contact_info' not in profile_data:
                    profile_data['contact_info'] = {}
                profile_data['contact_info']['emails'] = additional_emails
            
            if additional_emails:
                logger.info(f"[EMAIL] Found {len(additional_emails)} additional emails from profile sections")
            
            # ========== EXTRACT ADDITIONAL PHONES FROM ALL SECTIONS ==========
            # This extracts phones from about, headline, experience, posts, projects etc.
            # Contact info phones are preserved - this ADDS to them
            additional_phones = self._extract_phones_from_all_sections(profile_data, all_text)
            
            # Merge with contact info phones (contact info phones take priority)
            if 'contact_info' in profile_data and profile_data['contact_info']:
                contact_phones = profile_data['contact_info'].get('phones', [])
                if contact_phones and contact_phones != ['N/A']:
                    # Start with contact info phones
                    all_phones = list(contact_phones)
                    # Add additional phones that aren't already present
                    for phone in additional_phones:
                        # Normalize for comparison (remove spaces, dashes)
                        phone_normalized = re.sub(r'[\s\-\.\(\)]', '', phone)
                        existing_normalized = [re.sub(r'[\s\-\.\(\)]', '', p) for p in all_phones]
                        if phone_normalized not in existing_normalized:
                            all_phones.append(phone)
                    profile_data['contact_info']['phones'] = all_phones if all_phones else ['N/A']
                elif additional_phones:
                    # No contact info phones, use additional ones
                    profile_data['contact_info']['phones'] = additional_phones
            elif additional_phones:
                # No contact info at all, create minimal contact_info with phones
                if 'contact_info' not in profile_data:
                    profile_data['contact_info'] = {}
                profile_data['contact_info']['phones'] = additional_phones
            
            if additional_phones:
                logger.info(f"[PHONE] Found {len(additional_phones)} additional phones from profile sections")
            
            # ========== EXTRACT ADDITIONAL GITHUB URLs FROM ALL SECTIONS ==========
            # This extracts github URLs from about, headline, experience, posts, projects etc.
            # Contact info github_urls are preserved - this ADDS to them
            additional_github = self._extract_github_from_all_sections(profile_data, all_text)
            
            # Merge with contact info github_urls (contact info takes priority)
            if 'contact_info' in profile_data and profile_data['contact_info']:
                contact_github = profile_data['contact_info'].get('github_urls', [])
                if contact_github and contact_github != ['N/A']:
                    # Start with contact info github_urls
                    all_github = list(contact_github)
                    # Add additional github URLs that aren't already present
                    for gh in additional_github:
                        gh_normalized = gh.lower().rstrip('/')
                        existing_normalized = [g.lower().rstrip('/') for g in all_github]
                        if gh_normalized not in existing_normalized:
                            all_github.append(gh)
                    profile_data['contact_info']['github_urls'] = all_github if all_github else ['N/A']
                elif additional_github:
                    # No contact info github, use additional ones
                    profile_data['contact_info']['github_urls'] = additional_github
            elif additional_github:
                # No contact info at all, create minimal contact_info with github
                if 'contact_info' not in profile_data:
                    profile_data['contact_info'] = {}
                profile_data['contact_info']['github_urls'] = additional_github
            
            if additional_github:
                logger.info(f"[GITHUB] Found {len(additional_github)} additional GitHub URLs from profile sections")
            
            # ========== EXTRACT ADDITIONAL TWITTER FROM ALL SECTIONS ==========
            # This extracts twitter handles/URLs from about, headline, experience, posts, projects etc.
            # Contact info twitter are preserved - this ADDS to them
            additional_twitter = self._extract_twitter_from_all_sections(profile_data, all_text)
            
            # Merge with contact info twitter (contact info takes priority)
            if 'contact_info' in profile_data and profile_data['contact_info']:
                contact_twitter = profile_data['contact_info'].get('twitter', [])
                if contact_twitter and contact_twitter != ['N/A']:
                    # Start with contact info twitter
                    all_twitter = list(contact_twitter)
                    # Add additional twitter that aren't already present
                    for tw in additional_twitter:
                        tw_normalized = tw.lower().lstrip('@').rstrip('/')
                        existing_normalized = [t.lower().lstrip('@').rstrip('/') for t in all_twitter]
                        if tw_normalized not in existing_normalized:
                            all_twitter.append(tw)
                    profile_data['contact_info']['twitter'] = all_twitter if all_twitter else ['N/A']
                elif additional_twitter:
                    # No contact info twitter, use additional ones
                    profile_data['contact_info']['twitter'] = additional_twitter
            elif additional_twitter:
                # No contact info at all, create minimal contact_info with twitter
                if 'contact_info' not in profile_data:
                    profile_data['contact_info'] = {}
                profile_data['contact_info']['twitter'] = additional_twitter
            
            if additional_twitter:
                logger.info(f"[TWITTER] Found {len(additional_twitter)} additional Twitter handles from profile sections")
            
            # ========== EXTRACT ADDITIONAL INSTAGRAM FROM ALL SECTIONS ==========
            # This extracts instagram handles/URLs from about, headline, experience, posts, projects etc.
            # Contact info instagram are preserved - this ADDS to them
            additional_instagram = self._extract_instagram_from_all_sections(profile_data, all_text)
            
            # Merge with contact info instagram (contact info takes priority)
            if 'contact_info' in profile_data and profile_data['contact_info']:
                contact_instagram = profile_data['contact_info'].get('instagram', [])
                if contact_instagram and contact_instagram != ['N/A']:
                    # Start with contact info instagram
                    all_instagram = list(contact_instagram)
                    # Add additional instagram that aren't already present
                    for ig in additional_instagram:
                        ig_normalized = ig.lower().lstrip('@').rstrip('/')
                        existing_normalized = [i.lower().lstrip('@').rstrip('/') for i in all_instagram]
                        if ig_normalized not in existing_normalized:
                            all_instagram.append(ig)
                    profile_data['contact_info']['instagram'] = all_instagram if all_instagram else ['N/A']
                elif additional_instagram:
                    # No contact info instagram, use additional ones
                    profile_data['contact_info']['instagram'] = additional_instagram
            elif additional_instagram:
                # No contact info at all, create minimal contact_info with instagram
                if 'contact_info' not in profile_data:
                    profile_data['contact_info'] = {}
                profile_data['contact_info']['instagram'] = additional_instagram
            
            if additional_instagram:
                logger.info(f"[INSTAGRAM] Found {len(additional_instagram)} additional Instagram handles from profile sections")
            
            # ========== EXTRACT ADDITIONAL FACEBOOK FROM ALL SECTIONS ==========
            # This extracts facebook URLs from about, headline, experience, posts, projects etc.
            # Contact info facebook are preserved - this ADDS to them
            additional_facebook = self._extract_facebook_from_all_sections(profile_data, all_text)
            
            # Merge with contact info facebook (contact info takes priority)
            if 'contact_info' in profile_data and profile_data['contact_info']:
                contact_facebook = profile_data['contact_info'].get('facebook', [])
                if contact_facebook and contact_facebook != ['N/A']:
                    # Start with contact info facebook
                    all_facebook = list(contact_facebook)
                    # Add additional facebook that aren't already present
                    for fb in additional_facebook:
                        fb_normalized = fb.lower().rstrip('/')
                        existing_normalized = [f.lower().rstrip('/') for f in all_facebook]
                        if fb_normalized not in existing_normalized:
                            all_facebook.append(fb)
                    profile_data['contact_info']['facebook'] = all_facebook if all_facebook else ['N/A']
                elif additional_facebook:
                    # No contact info facebook, use additional ones
                    profile_data['contact_info']['facebook'] = additional_facebook
            elif additional_facebook:
                # No contact info at all, create minimal contact_info with facebook
                if 'contact_info' not in profile_data:
                    profile_data['contact_info'] = {}
                profile_data['contact_info']['facebook'] = additional_facebook
            
            if additional_facebook:
                logger.info(f"[FACEBOOK] Found {len(additional_facebook)} additional Facebook URLs from profile sections")
            
            # ========== EXTRACT ADDITIONAL WHATSAPP FROM ALL SECTIONS ==========
            # This extracts whatsapp numbers/links from about, headline, experience, posts, projects etc.
            # Contact info whatsapp are preserved - this ADDS to them
            additional_whatsapp = self._extract_whatsapp_from_all_sections(profile_data, all_text)
            
            # Merge with contact info whatsapp (contact info takes priority)
            if 'contact_info' in profile_data and profile_data['contact_info']:
                contact_whatsapp = profile_data['contact_info'].get('whatsapp', [])
                if contact_whatsapp and contact_whatsapp != ['N/A']:
                    # Start with contact info whatsapp
                    all_whatsapp = list(contact_whatsapp)
                    # Add additional whatsapp that aren't already present
                    for wa in additional_whatsapp:
                        wa_normalized = re.sub(r'[^\d+]', '', wa)
                        existing_normalized = [re.sub(r'[^\d+]', '', w) for w in all_whatsapp]
                        if wa_normalized not in existing_normalized:
                            all_whatsapp.append(wa)
                    profile_data['contact_info']['whatsapp'] = all_whatsapp if all_whatsapp else ['N/A']
                elif additional_whatsapp:
                    # No contact info whatsapp, use additional ones
                    profile_data['contact_info']['whatsapp'] = additional_whatsapp
            elif additional_whatsapp:
                # No contact info at all, create minimal contact_info with whatsapp
                if 'contact_info' not in profile_data:
                    profile_data['contact_info'] = {}
                profile_data['contact_info']['whatsapp'] = additional_whatsapp
            
            if additional_whatsapp:
                logger.info(f"[WHATSAPP] Found {len(additional_whatsapp)} additional WhatsApp numbers from profile sections")
            
            # ========== EXTRACT ADDITIONAL TELEGRAM FROM ALL SECTIONS ==========
            # This extracts telegram handles/links from about, headline, experience, posts, projects etc.
            # Contact info telegram are preserved - this ADDS to them
            additional_telegram = self._extract_telegram_from_all_sections(profile_data, all_text)
            
            # Merge with contact info telegram (contact info takes priority)
            if 'contact_info' in profile_data and profile_data['contact_info']:
                contact_telegram = profile_data['contact_info'].get('telegram', [])
                if contact_telegram and contact_telegram != ['N/A']:
                    # Start with contact info telegram
                    all_telegram = list(contact_telegram)
                    # Add additional telegram that aren't already present
                    for tg in additional_telegram:
                        tg_normalized = tg.lower().lstrip('@').rstrip('/')
                        existing_normalized = [t.lower().lstrip('@').rstrip('/') for t in all_telegram]
                        if tg_normalized not in existing_normalized:
                            all_telegram.append(tg)
                    profile_data['contact_info']['telegram'] = all_telegram if all_telegram else ['N/A']
                elif additional_telegram:
                    # No contact info telegram, use additional ones
                    profile_data['contact_info']['telegram'] = additional_telegram
            elif additional_telegram:
                # No contact info at all, create minimal contact_info with telegram
                if 'contact_info' not in profile_data:
                    profile_data['contact_info'] = {}
                profile_data['contact_info']['telegram'] = additional_telegram
            
            if additional_telegram:
                logger.info(f"[TELEGRAM] Found {len(additional_telegram)} additional Telegram handles from profile sections")
            
            # ========== EXTRACT ADDITIONAL SKYPE FROM ALL SECTIONS ==========
            # This extracts skype handles from about, headline, experience, posts, projects etc.
            # Contact info skype are preserved - this ADDS to them
            additional_skype = self._extract_skype_from_all_sections(profile_data, all_text)
            
            # Merge with contact info skype (contact info takes priority)
            if 'contact_info' in profile_data and profile_data['contact_info']:
                contact_skype = profile_data['contact_info'].get('skype', [])
                if contact_skype and contact_skype != ['N/A']:
                    # Start with contact info skype
                    all_skype = list(contact_skype)
                    # Add additional skype that aren't already present
                    for sk in additional_skype:
                        sk_normalized = sk.lower().strip()
                        existing_normalized = [s.lower().strip() for s in all_skype]
                        if sk_normalized not in existing_normalized:
                            all_skype.append(sk)
                    profile_data['contact_info']['skype'] = all_skype if all_skype else ['N/A']
                elif additional_skype:
                    # No contact info skype, use additional ones
                    profile_data['contact_info']['skype'] = additional_skype
            elif additional_skype:
                # No contact info at all, create minimal contact_info with skype
                if 'contact_info' not in profile_data:
                    profile_data['contact_info'] = {}
                profile_data['contact_info']['skype'] = additional_skype
            
            if additional_skype:
                logger.info(f"[SKYPE] Found {len(additional_skype)} additional Skype handles from profile sections")
            
            # ========== EXTRACT ADDITIONAL YOUTUBE FROM ALL SECTIONS ==========
            # This extracts youtube channel links from about, headline, experience, posts, projects etc.
            # Contact info youtube are preserved - this ADDS to them
            additional_youtube = self._extract_youtube_from_all_sections(profile_data, all_text)
            
            # Merge with contact info youtube (contact info takes priority)
            if 'contact_info' in profile_data and profile_data['contact_info']:
                contact_youtube = profile_data['contact_info'].get('youtube', [])
                if contact_youtube and contact_youtube != ['N/A']:
                    # Start with contact info youtube
                    all_youtube = list(contact_youtube)
                    # Add additional youtube that aren't already present
                    for yt in additional_youtube:
                        yt_normalized = yt.lower().rstrip('/').replace('www.', '')
                        existing_normalized = [y.lower().rstrip('/').replace('www.', '') for y in all_youtube]
                        if yt_normalized not in existing_normalized:
                            all_youtube.append(yt)
                    profile_data['contact_info']['youtube'] = all_youtube if all_youtube else ['N/A']
                elif additional_youtube:
                    # No contact info youtube, use additional ones
                    profile_data['contact_info']['youtube'] = additional_youtube
            elif additional_youtube:
                # No contact info at all, create minimal contact_info with youtube
                if 'contact_info' not in profile_data:
                    profile_data['contact_info'] = {}
                profile_data['contact_info']['youtube'] = additional_youtube
            
            if additional_youtube:
                logger.info(f"[YOUTUBE] Found {len(additional_youtube)} additional YouTube links from profile sections")
            
            # ========== EXTRACT ADDITIONAL TWITTER_URL FROM ALL SECTIONS ==========
            # This extracts twitter URLs/handles from about, headline, experience, posts, projects etc.
            # Contact info twitter_url are preserved - this ADDS to them
            additional_twitter = self._extract_twitter_url_from_all_sections(profile_data, all_text)
            
            # Merge with contact info twitter_url (contact info takes priority)
            if 'contact_info' in profile_data and profile_data['contact_info']:
                contact_twitter = profile_data['contact_info'].get('twitter_url', [])
                if contact_twitter and contact_twitter != ['N/A']:
                    # Start with contact info twitter
                    all_twitter = list(contact_twitter)
                    # Add additional twitter that aren't already present
                    for tw in additional_twitter:
                        tw_normalized = tw.lower().rstrip('/').replace('www.', '').replace('x.com', 'twitter.com')
                        existing_normalized = [t.lower().rstrip('/').replace('www.', '').replace('x.com', 'twitter.com') for t in all_twitter]
                        if tw_normalized not in existing_normalized:
                            all_twitter.append(tw)
                    profile_data['contact_info']['twitter_url'] = all_twitter if all_twitter else ['N/A']
                elif additional_twitter:
                    # No contact info twitter, use additional ones
                    profile_data['contact_info']['twitter_url'] = additional_twitter
            elif additional_twitter:
                # No contact info at all, create minimal contact_info with twitter
                if 'contact_info' not in profile_data:
                    profile_data['contact_info'] = {}
                profile_data['contact_info']['twitter_url'] = additional_twitter
            
            if additional_twitter:
                logger.info(f"[TWITTER] Found {len(additional_twitter)} additional Twitter URLs from profile sections")
            
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
            # Method 1: Direct JavaScript extraction using LinkedIn's specific selectors
            name = await page.evaluate("""
                () => {
                    // PRIORITY 1: LinkedIn profile name h1 with exact class match
                    // This is the most reliable selector for profile names
                    const primarySelectors = [
                        'h1.text-heading-xlarge',
                        'h1.inline.t-24.v-align-middle.break-words',
                        'h1[data-generated-suggestion-target]'
                    ];
                    
                    const invalidTexts = ['keyboard shortcuts', 'skip to', 'sign in', 'join now', 
                                         'for business', 'search', 'home', 'my network', 'messaging',
                                         'notifications', 'jobs', 'learning', 'experience', 'education',
                                         'accessibility', 'linkedin', 'help center', 'settings'];
                    
                    for (const selector of primarySelectors) {
                        try {
                            const el = document.querySelector(selector);
                            if (el) {
                                const text = el.innerText.trim();
                                const isInvalid = invalidTexts.some(inv => text.toLowerCase().includes(inv));
                                
                                if (text && text.length > 2 && text.length < 80 && !isInvalid) {
                                    const words = text.split(/\\s+/).filter(w => w);
                                    if (words.length >= 1 && words.length <= 6 && /[a-zA-Z]/.test(text)) {
                                        return text;
                                    }
                                }
                            }
                        } catch(e) {}
                    }
                    
                    // PRIORITY 2: Profile card section h1
                    const profileSection = document.querySelector('.pv-text-details__left-panel, .ph5.pb5');
                    if (profileSection) {
                        const h1 = profileSection.querySelector('h1');
                        if (h1) {
                            const text = h1.innerText.trim();
                            const isInvalid = invalidTexts.some(inv => text.toLowerCase().includes(inv));
                            if (text && text.length > 2 && text.length < 80 && !isInvalid) {
                                const words = text.split(/\\s+/).filter(w => w);
                                if (words.length >= 1 && words.length <= 6 && /[a-zA-Z]/.test(text)) {
                                    return text;
                                }
                            }
                        }
                    }
                    
                    // PRIORITY 3: First h1 in artdeco-card that looks like a name
                    const cards = document.querySelectorAll('section.artdeco-card');
                    for (const card of cards) {
                        const h1 = card.querySelector('h1');
                        if (h1) {
                            const text = h1.innerText.trim();
                            const isInvalid = invalidTexts.some(inv => text.toLowerCase().includes(inv));
                            if (text && text.length > 2 && text.length < 80 && !isInvalid) {
                                const words = text.split(/\\s+/).filter(w => w);
                                if (words.length >= 1 && words.length <= 6 && /[a-zA-Z]/.test(text)) {
                                    return text;
                                }
                            }
                        }
                    }
                    
                    return null;
                }
            """)
            
            if name and len(name) > 2 and len(name) < 100 and 'keyboard' not in name.lower():
                return name.strip()
            
            # Method 2: Extract from page title (LinkedIn profile pages have "Name | LinkedIn")
            try:
                page_title = await page.title()
                if page_title and '|' in page_title:
                    name_from_title = page_title.split('|')[0].strip()
                    # Remove common suffixes like (He/Him) or - Title
                    name_from_title = re.sub(r'\s*\([^)]*\)\s*$', '', name_from_title).strip()
                    name_from_title = name_from_title.split(' - ')[0].strip()
                    if name_from_title and len(name_from_title) > 2 and len(name_from_title) < 80:
                        if 'keyboard' not in name_from_title.lower() and 'linkedin' not in name_from_title.lower():
                            return name_from_title
            except:
                pass
            
            # Method 3: Try fallback text extraction  
            name = await self._extract_name_fallback(all_text)
            if name and 'keyboard' not in name.lower():
                return name
            
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
        """Extract About section using JavaScript first, then text fallback"""
        try:
            # Method 1: JavaScript extraction targeting the About section directly
            about = await page.evaluate("""
                () => {
                    // LinkedIn's About section is in a specific card/section
                    // Look for section with id containing 'about'
                    const aboutSection = document.querySelector('section[id*="about"], div[id*="about"]');
                    if (aboutSection) {
                        // Find the main text content (usually in a div with specific class)
                        const textDiv = aboutSection.querySelector('.display-flex.ph5.pv3, .pv-shared-text-with-see-more, [class*="inline-show-more"]');
                        if (textDiv) {
                            const text = textDiv.innerText.trim();
                            if (text && text.length > 10) {
                                // Clean up "see more" text
                                return text.replace(/\\s*…see more\\s*$/i, '').trim();
                            }
                        }
                        // Fallback: get all text from section but exclude header
                        const allText = aboutSection.innerText.trim();
                        if (allText) {
                            // Remove the "About" header
                            let cleaned = allText.replace(/^About\\s*/i, '').trim();
                            // Remove "see more" suffix
                            cleaned = cleaned.replace(/\\s*…see more\\s*$/i, '').trim();
                            if (cleaned.length > 10) {
                                return cleaned;
                            }
                        }
                    }
                    
                    // Method 2: Look for About header and get sibling content
                    const headers = document.querySelectorAll('h2, div[class*="header"]');
                    for (const header of headers) {
                        if (header.innerText.trim().toLowerCase() === 'about') {
                            // Get the parent section
                            const section = header.closest('section, .artdeco-card');
                            if (section) {
                                const textContent = section.querySelector('[class*="inline-show-more"], .pv-shared-text-with-see-more');
                                if (textContent) {
                                    let text = textContent.innerText.trim();
                                    text = text.replace(/\\s*…see more\\s*$/i, '').trim();
                                    if (text && text.length > 10) {
                                        return text;
                                    }
                                }
                            }
                        }
                    }
                    
                    return null;
                }
            """)
            
            if about and len(about) > 10:
                # Validate the about doesn't contain footer/navigation junk
                junk_indicators = ['accessibility', 'talent solutions', 'professional community', 
                                  'linkedin corporation', 'select language', 'help center',
                                  'marketing solutions', 'privacy & terms', 'ad choices']
                about_lower = about.lower()
                if not any(junk in about_lower for junk in junk_indicators):
                    return about.strip()
            
            # Method 2: Text-based fallback with strict boundaries
            lines = all_text.split('\n')
            about_started = False
            about_text = []
            
            # Keywords that end the about section
            section_ends = ['Experience', 'Education', 'Skills', 'Licenses', 'Certifications', 
                           'Activity', 'Projects', 'Volunteer', 'Interests', 'Organizations',
                           'Show all', 'See all']
            
            for i, line in enumerate(lines):
                line_stripped = line.strip()
                
                if line_stripped == 'About':
                    about_started = True
                    continue
                
                if about_started:
                    # Stop at next section
                    if any(line_stripped.startswith(section) for section in section_ends):
                        break
                    
                    # Skip junk lines
                    if line_stripped and not any(x in line_stripped for x in ['http', 'button', 'Follow', '…see more']):
                        # Skip lines that are clearly navigation/footer text
                        if not any(junk in line_stripped.lower() for junk in ['accessibility', 'linkedin corporation', 'select language', 'help center']):
                            about_text.append(line_stripped)
                    
                    # Limit about to reasonable length (avoid grabbing entire page)
                    if len(' '.join(about_text)) > 2000:
                        break
            
            about_result = ' '.join(about_text).strip()
            if about_result and len(about_result) > 10:
                # Final validation
                junk_indicators = ['accessibility', 'talent solutions', 'professional community', 
                                  'linkedin corporation', 'select language', 'help center']
                if not any(junk in about_result.lower() for junk in junk_indicators):
                    return about_result
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting about: {e}")
            return None
    
    async def _extract_experience(self, page: Page, all_text: str) -> List[Dict]:
        """Extract experience entries with full structured data from LinkedIn profile"""
        experiences = []
        original_url = page.url
        try:
            # ========== AGENT-STYLE APPROACH (PRIMARY) ==========
            # Use the scrape_agent's universal experience extraction if available
            # This handles: Show all experiences, Skills buttons, See more buttons
            if hasattr(self, 'scrape_agent') and self.scrape_agent:
                try:
                    logger.info("[EXPERIENCE] Using agent-style extraction for universal compatibility")
                    agent_experiences = await self.scrape_agent.extract_all_experiences_agent(original_url)
                    if agent_experiences and len(agent_experiences) > 0:
                        logger.info(f"[EXPERIENCE] Agent extracted {len(agent_experiences)} experiences successfully")
                        return agent_experiences
                    else:
                        logger.debug("[EXPERIENCE] Agent returned no experiences, falling back to standard extraction")
                except Exception as e:
                    logger.warning(f"[EXPERIENCE] Agent extraction failed, falling back: {e}")
            
            # ========== FALLBACK: Standard extraction method ==========
            # AGGRESSIVE: Always try to navigate to experience details page if we're on a profile page
            # This ensures we get ALL experiences, not just the ones visible on the main profile
            show_all_navigated = False
            current_url = page.url
            
            # Check if we're already on experience details page
            is_experience_page = '/details/experience' in current_url
            
            if not is_experience_page:
                try:
                    # Method 1: Try to find "Show all experiences" button/link
                    show_all_href = None
                    show_all_info = await page.evaluate("""
                        () => {
                            // ULTRA-UNIVERSAL: Find ANY link to experience details page
                            const allClickable = document.querySelectorAll('a, button, [role="button"]');
                            
                            // Priority 1: Find by text pattern (most reliable)
                            for (const el of allClickable) {
                                const text = (el.textContent || '').trim().toLowerCase();
                                const ariaLabel = (el.getAttribute('aria-label') || '').trim().toLowerCase();
                                const combinedText = text + ' ' + ariaLabel;
                                
                                // Check for "show all X experiences" pattern
                                if (combinedText.match(/show\\s+all\\s+\\d+\\s+experience/i) ||
                                    combinedText.match(/see\\s+all\\s+\\d+\\s+experience/i) ||
                                    (combinedText.includes('show all') && combinedText.includes('experience')) ||
                                    (combinedText.includes('see all') && combinedText.includes('experience'))) {
                                    
                                    let href = (el.getAttribute('href') || '').trim();
                                    if (!href || !href.includes('/details/experience')) {
                                        const parentLink = el.closest('a[href*="/details/experience"]');
                                        if (parentLink) {
                                            href = (parentLink.getAttribute('href') || '').trim();
                                        }
                                    }
                                    
                                    if (href && href.includes('/details/experience')) {
                                        return href;
                                    }
                                }
                            }
                            
                            // Priority 2: Find by ID pattern
                            for (const el of allClickable) {
                                const id = (el.getAttribute('id') || '').trim().toLowerCase();
                                if (id && (id.includes('see-all-experiences') || id.includes('see-all-positions'))) {
                                    const href = (el.getAttribute('href') || '').trim();
                                    if (href && href.includes('/details/experience')) {
                                        return href;
                                    }
                                }
                            }
                            
                            // Priority 3: Find ANY link to /details/experience in footer
                            const footers = document.querySelectorAll('[class*="footer"]');
                            for (const footer of footers) {
                                const links = footer.querySelectorAll('a');
                                for (const link of links) {
                                    const href = (link.getAttribute('href') || '').trim();
                                    if (href && href.includes('/details/experience')) {
                                        return href;
                                    }
                                }
                            }
                            
                            // Priority 4: Find ANY link to /details/experience anywhere
                            for (const el of allClickable) {
                                const href = (el.getAttribute('href') || '').trim();
                                if (href && href.includes('/details/experience') && !href.includes('#')) {
                                    return href;
                                }
                            }
                            
                            return null;
                        }
                    """)
                    
                    if show_all_info:
                        show_all_href = show_all_info
                        if not show_all_href.startswith('http'):
                            show_all_href = f"https://www.linkedin.com{show_all_href}"
                    else:
                        # Last resort: Try to construct the URL from current profile URL
                        if '/in/' in current_url:
                            profile_slug = current_url.split('/in/')[-1].split('/')[0].split('?')[0]
                            show_all_href = f"https://www.linkedin.com/in/{profile_slug}/details/experience"
                    
                    # Navigate to experience details page
                    if show_all_href:
                        try:
                            logger.info(f"[EXPERIENCE] Navigating to experience details page: {show_all_href}")
                            await page.goto(show_all_href, wait_until='domcontentloaded', timeout=30000)
                            await asyncio.sleep(3)
                            show_all_navigated = True
                        except Exception as e:
                            logger.debug(f"Error navigating to experience page: {e}")
                            
                except Exception as e:
                    logger.debug(f"Error finding experience details link: {e}")
                
                # Scroll to load all experiences (lazy loading) - ALWAYS scroll, even if we didn't navigate
                # This ensures we load all lazy-loaded content
                logger.debug("[EXPERIENCE] Scrolling to load all experiences...")
                for scroll_attempt in range(20):  # Increased attempts for better loading
                    await page.evaluate("""
                        () => {
                            // Scroll main window
                            window.scrollTo(0, document.body.scrollHeight);
                            
                            // Scroll all scrollable containers
                            const containers = document.querySelectorAll(
                                '.scaffold-finite-scroll__content, ' +
                                '.pvs-list__container, ' +
                                '.artdeco-modal__content, ' +
                                '.pvs-modal__content, ' +
                                'main, ' +
                                '[class*="scroll"]'
                            );
                            containers.forEach(container => {
                                try {
                                    container.scrollTop = container.scrollHeight;
                                } catch(e) {}
                            });
                        }
                    """)
                    await asyncio.sleep(1)
                    
                    # Check if "Show more results" button exists and click it
                    try:
                        show_more_btn = await page.query_selector(
                            'button.scaffold-finite-scroll__load-button, ' +
                            'button[aria-label*="Show more"], ' +
                            'button[aria-label*="Load more"], ' +
                            'button:has-text("Show more"), ' +
                            'button:has-text("Load more")'
                        )
                        if show_more_btn:
                            is_visible = await show_more_btn.is_visible()
                            if is_visible:
                                await show_more_btn.scroll_into_view_if_needed()
                                await show_more_btn.click()
                                await asyncio.sleep(2)
                                logger.debug(f"[EXPERIENCE] Clicked 'Show more results' button (attempt {scroll_attempt + 1})")
                            else:
                                break
                        else:
                            break
                    except Exception as e:
                        logger.debug(f"[EXPERIENCE] Error checking for 'Show more' button: {e}")
                        pass
            
            # Extract experiences using JavaScript from experience detail page or main profile
            experiences = await page.evaluate("""
                () => {
                    const experiences = [];
                    
                    // Find all experience items - works on both main profile and details page
                    // First, identify if we're in an experience section/page
                    const isExperiencePage = window.location.href.includes('/details/experience');
                    
                    // Get experience section (avoid education section)
                    let expSection = null;
                    const allSections = document.querySelectorAll('section');
                    for (const section of allSections) {
                        const sectionId = section.querySelector('#experience');
                        const sectionHeader = section.querySelector('h2');
                        if (sectionId || (sectionHeader && sectionHeader.textContent.includes('Experience'))) {
                            const eduId = section.querySelector('#education');
                            if (!eduId) {  // Make sure it's not education
                                expSection = section;
                                break;
                            }
                        }
                    }
                    
                    // ULTRA-UNIVERSAL: Get ALL possible experience items using aggressive search
                    // This works for ANY HTML structure, ANY class names, ANY layout
                    let expItems = [];
                    
                    if (isExperiencePage) {
                        // On experience details page, get ALL list items - be VERY aggressive
                        expItems = Array.from(document.querySelectorAll('li')).filter(li => {
                            // Filter out obviously non-experience items
                            const text = (li.textContent || '').toLowerCase();
                            // Skip navigation, footer, header items
                            if (text.includes('navigation') || text.includes('footer') || text.includes('header') ||
                                text.includes('menu') || text.includes('sidebar')) {
                                return false;
                            }
                            // Must have some content
                            return li.textContent.trim().length > 10;
                        });
                    } else {
                        // On main profile page - search in experience section
                        if (expSection) {
                            // Get ALL list items in experience section
                            expItems = Array.from(expSection.querySelectorAll('li')).filter(li => {
                                const text = (li.textContent || '').toLowerCase();
                                // Skip navigation/footer items
                                if (text.includes('navigation') || text.includes('footer') || text.includes('show all')) {
                                    return false;
                                }
                                return li.textContent.trim().length > 10;
                            });
                        } else {
                            // No section found - search by ID
                            const expSectionById = document.querySelector('#experience');
                            if (expSectionById) {
                                const section = expSectionById.closest('section') || expSectionById.parentElement;
                                if (section) {
                                    expItems = Array.from(section.querySelectorAll('li')).filter(li => {
                                        const text = (li.textContent || '').toLowerCase();
                                        if (text.includes('navigation') || text.includes('footer')) return false;
                                        return li.textContent.trim().length > 10;
                                    });
                                }
                            } else {
                                // Last resort: find ALL list items with profile-component-entity
                                // This is very aggressive but catches everything
                                const allItems = document.querySelectorAll('li');
                                expItems = Array.from(allItems).filter(item => {
                                    // Must have profile-component-entity (indicates it's a profile data item)
                                    const entity = item.querySelector('[data-view-name="profile-component-entity"]');
                                    if (!entity) return false;
                                    
                                    // Check if it's in experience context (not education, connections, etc.)
                                    const parentSection = item.closest('section');
                                    if (parentSection) {
                                        const sectionText = (parentSection.textContent || '').toLowerCase();
                                        const sectionId = parentSection.querySelector('#experience, #education, #connections');
                                        if (sectionId) {
                                            const id = sectionId.getAttribute('id');
                                            if (id === 'education' || id === 'connections') return false;
                                        }
                                        // Check section header
                                        const header = parentSection.querySelector('h2, h3');
                                        if (header) {
                                            const headerText = (header.textContent || '').toLowerCase();
                                            if (headerText.includes('education') || 
                                                headerText.includes('connection') ||
                                                headerText.includes('following')) {
                                                return false;
                                            }
                                        }
                                    }
                                    
                                    // Check if text suggests experience (has employment types, dates, company info)
                                    const text = item.textContent || '';
                                    return text.includes('Full-time') || 
                                           text.includes('Part-time') ||
                                           text.includes('Internship') ||
                                           text.includes('Contract') ||
                                           text.includes('Self-employed') ||
                                           text.includes('·') || // Common separator in LinkedIn
                                           /\\d{4}/.test(text) || // Has year dates
                                           text.includes('yrs') ||
                                           text.includes('mos') ||
                                           text.includes('yr') ||
                                           text.includes('mo');
                                });
                            }
                        }
                    }
                    
                    for (const item of expItems) {
                        // STRICT: Make sure we're in the experience section
                        const parentSection = item.closest('section');
                        if (!parentSection && !isExperiencePage) continue;
                        
                        // Check section ID (only if we have a parent section)
                        if (parentSection) {
                            const expId = parentSection.querySelector('#experience');
                            const eduId = parentSection.querySelector('#education');
                            if (eduId) continue;  // Skip if in education section
                            
                            // Check section header - must be "Experience" exactly
                            // But if we have expId, trust it (section ID is more reliable)
                            if (!expId) {
                                const sectionHeader = parentSection.querySelector('h2');
                                if (sectionHeader) {
                                    const headerText = sectionHeader.textContent.trim().toLowerCase();
                                    // Must be experience section, not education, connections, following, etc.
                                    if (!headerText.includes('experience') || 
                                        headerText.includes('education') ||
                                        headerText.includes('connection') ||
                                        headerText.includes('following') ||
                                        headerText.includes('people') ||
                                        headerText.includes('recommendation')) {
                                        continue;
                                    }
                                } else {
                                    // No header and no experience ID - skip (unless on experience page)
                                    if (!isExperiencePage) continue;
                                }
                            }
                        }
                        
                        // Check if this is an experience item - be VERY lenient
                        // Try to find entityDiv, but don't require it (some profiles don't have it)
                        let entityDiv = item.querySelector('[data-view-name="profile-component-entity"]');
                        
                        // Also check if the item itself has the data-view-name attribute
                        if (!entityDiv && item.hasAttribute('data-view-name')) {
                            const viewName = item.getAttribute('data-view-name');
                            if (viewName === 'profile-component-entity') {
                                entityDiv = item;
                            }
                        }
                        
                        // Also check nested structure - sometimes entityDiv is deeper
                        if (!entityDiv) {
                            entityDiv = item.querySelector('div[data-view-name="profile-component-entity"]');
                        }
                        
                        // For nested positions (like SQ Group), check if parent has entityDiv
                        if (!entityDiv) {
                            const parentItem = item.closest('li.artdeco-list__item, li');
                            if (parentItem && parentItem !== item) {
                                entityDiv = parentItem.querySelector('[data-view-name="profile-component-entity"]');
                            }
                        }
                        
                        // If no entityDiv, use the item itself as the container (for simpler structures)
                        if (!entityDiv) {
                            entityDiv = item;
                        }
                        
                        // STRICT: Check if this looks like a connection/recommendation item
                        // Connections/recommendations often have person names as titles
                        const itemText = item.textContent || '';
                        // Skip if it looks like a person name pattern (2-3 words, capitalized, no company info)
                        const titleCandidate = entityDiv.querySelector('.t-bold span[aria-hidden="true"]');
                        if (titleCandidate) {
                            const titleText = titleCandidate.textContent.trim();
                            // Check if it's a person name (pattern: FirstName LastName or FirstName MiddleName LastName)
                            const namePattern = /^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}$/;
                            if (namePattern.test(titleText) && titleText.length < 50) {
                                // Check if there's no company info nearby - if no company, it's likely a person name
                                const hasCompanyInfo = itemText.includes('·') && 
                                    (itemText.includes('Full-time') || 
                                     itemText.includes('Part-time') || 
                                     itemText.includes('Internship') ||
                                     itemText.includes('Contract') ||
                                     /company/i.test(itemText));
                                if (!hasCompanyInfo) {
                                    continue;  // Skip person names without company info
                                }
                            }
                        }
                        
                        // Also check if this is nested under another experience (sub-position)
                        // Nested positions are valid and should be extracted
                        const isNested = item.closest('.pvs-entity__sub-components') !== null;
                        // This is fine - nested positions are still valid experiences
                        
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
                            accomplishments: []
                        };
                        
                        // Extract job title - ULTRA-UNIVERSAL method that works for ALL structures
                        // Try multiple methods, be VERY aggressive
                        let titleEl = null;
                        const itemText = item.textContent || '';
                        
                        // Method 1: Try standard selectors in entityDiv
                        if (entityDiv) {
                            titleEl = entityDiv.querySelector('.t-bold span[aria-hidden="true"], .t-bold, h3, h2, [class*="bold"]');
                        }
                        
                        // Method 2: Try in nested structures
                        if (!titleEl) {
                            const nestedEntity = item.querySelector('.pvs-entity__sub-components [data-view-name="profile-component-entity"], .pvs-entity__sub-components');
                            if (nestedEntity) {
                                titleEl = nestedEntity.querySelector('.t-bold span[aria-hidden="true"], .t-bold, h3');
                            }
                        }
                        
                        // Method 3: Try finding first bold text in item (for simpler structures)
                        if (!titleEl) {
                            const allBold = item.querySelectorAll('.t-bold, [class*="bold"], strong, b, h2, h3, h4');
                            for (const bold of allBold) {
                                const text = (bold.textContent || '').trim();
                                // Skip if it's clearly not a title (dates, employment type, etc.)
                                if (text && text.length > 2 && text.length < 100 && 
                                    !text.toLowerCase().includes('full-time') && 
                                    !text.toLowerCase().includes('part-time') &&
                                    !text.toLowerCase().includes('internship') &&
                                    !text.toLowerCase().includes('contract') &&
                                    !text.toLowerCase().includes('self-employed') &&
                                    !text.match(/\\d{4}/) &&
                                    !text.includes('·') &&
                                    !text.toLowerCase().includes('experience') &&
                                    !text.toLowerCase().includes('show all') &&
                                    !text.toLowerCase().includes('see all')) {
                                    titleEl = bold;
                                    break;
                                }
                            }
                        }
                        
                        // Method 4: Extract from text patterns (for very simple structures)
                        if (!titleEl) {
                            // Look for text that appears before company/employment type
                            const lines = itemText.split('\\n').map(l => l.trim()).filter(l => l.length > 0);
                            for (const line of lines) {
                                // Skip if it's employment type, dates, or location
                                if (!line.toLowerCase().includes('full-time') && 
                                    !line.toLowerCase().includes('part-time') &&
                                    !line.toLowerCase().includes('internship') &&
                                    !line.match(/\\d{4}/) &&
                                    !line.includes(' - ') &&
                                    !line.includes(' to ') &&
                                    !line.toLowerCase().includes('remote') &&
                                    !line.toLowerCase().includes('on-site') &&
                                    line.length > 2 && line.length < 100) {
                                    // This might be the title
                                    exp.title = line;
                                    break;
                                }
                            }
                        }
                        
                        if (titleEl) {
                            const titleText = (titleEl.textContent || '').trim();
                            if (titleText && titleText.length > 0) {
                                exp.title = titleText;
                            }
                        }
                        
                        // Extract company name and employment type - ULTRA-UNIVERSAL method
                        let companyFound = false;
                        
                        // Method 1: Direct span.t-14.t-normal > span[aria-hidden="true"]
                        if (entityDiv) {
                            const companySpans = entityDiv.querySelectorAll('span.t-14.t-normal > span[aria-hidden="true"], span[class*="normal"] > span[aria-hidden="true"]');
                            for (const span of companySpans) {
                                const companyText = span.textContent.trim();
                                // Skip if it looks like dates or location
                                if (companyText.includes(' - ') || companyText.includes(' to ') || /\\d{4}/.test(companyText)) {
                                    continue;
                                }
                                // Parse "Company Name · Employment Type" format
                                if (companyText.includes('·')) {
                                    const parts = companyText.split('·').map(p => p.trim());
                                    exp.company = parts[0] || 'N/A';
                                    exp.employment_type = parts[1] || 'N/A';
                                    companyFound = true;
                                    break;
                                } else if (companyText && companyText.length > 0 && companyText.length < 200) {
                                    exp.company = companyText;
                                    companyFound = true;
                                    break;
                                }
                            }
                        }
                        
                        // Method 1b: Try finding company from text patterns (for simpler structures)
                        if (!companyFound) {
                            const lines = itemText.split('\\n').map(l => l.trim()).filter(l => l.length > 0);
                            for (const line of lines) {
                                // Check if line contains employment type or company pattern
                                if (line.includes('·') || 
                                    line.toLowerCase().includes('full-time') ||
                                    line.toLowerCase().includes('part-time') ||
                                    line.toLowerCase().includes('internship') ||
                                    line.toLowerCase().includes('contract') ||
                                    line.toLowerCase().includes('self-employed') ||
                                    line.toLowerCase().includes('seasonal')) {
                                    
                                    if (line.includes('·')) {
                                        const parts = line.split('·').map(p => p.trim());
                                        // First part is usually company
                                        if (parts[0] && !parts[0].toLowerCase().includes('full-time') && 
                                            !parts[0].toLowerCase().includes('part-time')) {
                                            exp.company = parts[0];
                                            if (parts[1]) {
                                                exp.employment_type = parts[1];
                                            }
                                            companyFound = true;
                                            break;
                                        }
                                    } else {
                                        // Try to extract company name before employment type
                                        const empTypes = ['full-time', 'part-time', 'internship', 'contract', 'self-employed', 'seasonal'];
                                        for (const empType of empTypes) {
                                            if (line.toLowerCase().includes(empType)) {
                                                const parts = line.toLowerCase().split(empType);
                                                if (parts[0] && parts[0].trim().length > 0) {
                                                    exp.company = parts[0].trim();
                                                    exp.employment_type = empType;
                                                    companyFound = true;
                                                    break;
                                                }
                                            }
                                        }
                                        if (companyFound) break;
                                    }
                                }
                            }
                        }
                        
                        // Method 2: If not found, try finding company from link text or nearby text
                        if (!companyFound) {
                            // Look for company link
                            const companyLink = entityDiv.querySelector('a[href*="/company/"]');
                            if (companyLink) {
                                // Try to get company name from link's text or nearby text
                                const linkText = companyLink.textContent.trim();
                                if (linkText && linkText.length > 0 && linkText.length < 200) {
                                    exp.company = linkText;
                                    companyFound = true;
                                }
                            }
                        }
                        
                        // Method 3: Look for text that follows the title but isn't dates/location
                        if (!companyFound) {
                            const allTextSpans = entityDiv.querySelectorAll('span[aria-hidden="true"]');
                            let foundTitle = false;
                            for (const span of allTextSpans) {
                                const text = span.textContent.trim();
                                if (text === exp.title) {
                                    foundTitle = true;
                                    continue;
                                }
                                if (foundTitle && text && text.length > 0 && text.length < 200) {
                                    // Check if it's company info (contains · or employment type keywords)
                                    if (text.includes('·') || 
                                        text.toLowerCase().includes('full-time') ||
                                        text.toLowerCase().includes('part-time') ||
                                        text.toLowerCase().includes('internship') ||
                                        text.toLowerCase().includes('contract') ||
                                        text.toLowerCase().includes('self-employed')) {
                                        if (text.includes('·')) {
                                            const parts = text.split('·').map(p => p.trim());
                                            exp.company = parts[0] || 'N/A';
                                            exp.employment_type = parts[1] || 'N/A';
                                        } else {
                                            exp.company = text;
                                        }
                                        companyFound = true;
                                        break;
                                    }
                                }
                            }
                        }
                        
                        // Method 4: For nested positions, check parent item for company info
                        if (!companyFound) {
                            const parentItem = item.closest('li.artdeco-list__item');
                            if (parentItem && parentItem !== item) {
                                const parentEntity = parentItem.querySelector('[data-view-name="profile-component-entity"]');
                                if (parentEntity) {
                                    const parentCompanySpans = parentEntity.querySelectorAll('span.t-14.t-normal > span[aria-hidden="true"]');
                                    for (const span of parentCompanySpans) {
                                        const companyText = span.textContent.trim();
                                        if (companyText && !companyText.includes(' - ') && !companyText.includes(' to ') && !/\\d{4}/.test(companyText)) {
                                            if (companyText.includes('·')) {
                                                const parts = companyText.split('·').map(p => p.trim());
                                                exp.company = parts[0] || 'N/A';
                                                exp.employment_type = parts[1] || 'N/A';
                                            } else {
                                                exp.company = companyText;
                                            }
                                            companyFound = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Method 5: Handle cases where company name is shown as title (like "Mark Hughes", "SQ Group")
                        // Check if title is actually a company name (has duration, employment type, or nested positions)
                        if (!companyFound && exp.title && exp.title !== 'N/A') {
                            const itemText = item.textContent || '';
                            // Check for duration indicators (like "49 yrs 1 mo", "5 yrs 1 mo")
                            const hasDuration = itemText.includes('yrs') || 
                                              itemText.includes('mos') || 
                                              itemText.includes('yr') ||
                                              itemText.match(/\\d+\\s*(yrs?|mos?)/);
                            
                            // Check for employment type in the same item
                            const hasEmploymentType = itemText.includes('Full-time') ||
                                                      itemText.includes('Part-time') ||
                                                      itemText.includes('Internship') ||
                                                      itemText.includes('Contract') ||
                                                      itemText.includes('Self-employed');
                            
                            // Check if there are nested positions (indicates this is a company, not a title)
                            const hasNestedPositions = item.querySelector('.pvs-entity__sub-components [data-view-name="profile-component-entity"]') !== null;
                            
                            // If title has duration/employment type/nested positions, it's likely a company name
                            if ((hasDuration || hasEmploymentType || hasNestedPositions) && 
                                exp.title.length > 2 && exp.title.length < 150) {
                                exp.company = exp.title;
                                // Try to get actual title from nested positions
                                const nestedEntity = item.querySelector('.pvs-entity__sub-components [data-view-name="profile-component-entity"]');
                                if (nestedEntity) {
                                    const nestedTitle = nestedEntity.querySelector('.t-bold span[aria-hidden="true"]');
                                    if (nestedTitle) {
                                        exp.title = nestedTitle.textContent.trim();
                                    } else {
                                        exp.title = 'N/A'; // Will be extracted from nested positions later
                                    }
                                } else {
                                    exp.title = 'N/A';
                                }
                                companyFound = true;
                            }
                        }
                        
                        // Method 6: For nested positions, extract company from parent item
                        if (!companyFound) {
                            const parentItem = item.closest('li.artdeco-list__item');
                            if (parentItem && parentItem !== item) {
                                const parentEntity = parentItem.querySelector('[data-view-name="profile-component-entity"]');
                                if (parentEntity) {
                                    // Check if parent has company info
                                    const parentText = parentItem.textContent || '';
                                    // Look for company in parent's text spans
                                    const parentSpans = parentEntity.querySelectorAll('span.t-14.t-normal > span[aria-hidden="true"]');
                                    for (const span of parentSpans) {
                                        const text = span.textContent.trim();
                                        if (text && !text.includes(' - ') && !text.includes(' to ') && !/\\d{4}/.test(text)) {
                                            if (text.includes('·')) {
                                                const parts = text.split('·').map(p => p.trim());
                                                exp.company = parts[0] || 'N/A';
                                                exp.employment_type = parts[1] || 'N/A';
                                            } else if (text.length > 2 && text.length < 200) {
                                                exp.company = text;
                                            }
                                            if (exp.company && exp.company !== 'N/A') {
                                                companyFound = true;
                                                break;
                                            }
                                        }
                                    }
                                    
                                    // Also check parent title if it looks like company
                                    if (!companyFound) {
                                        const parentTitleEl = parentEntity.querySelector('.t-bold span[aria-hidden="true"]');
                                        if (parentTitleEl) {
                                            const parentTitle = parentTitleEl.textContent.trim();
                                            // If parent has duration/employment type, it's likely company
                                            if ((parentText.includes('yrs') || parentText.includes('mos') || 
                                                 parentText.includes('Full-time') || parentText.includes('Part-time')) &&
                                                parentTitle.length > 2 && parentTitle.length < 150) {
                                                exp.company = parentTitle;
                                                companyFound = true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Extract dates and duration - ULTRA-UNIVERSAL method
                        // Try multiple selectors and locations for dates
                        let datesEl = null;
                        if (entityDiv) {
                            datesEl = entityDiv.querySelector('.pvs-entity__caption-wrapper[aria-hidden="true"], [class*="caption"][aria-hidden="true"]');
                        }
                        if (!datesEl) {
                            // Try finding in nested structures
                            datesEl = item.querySelector('.pvs-entity__caption-wrapper[aria-hidden="true"], [class*="caption"][aria-hidden="true"]');
                        }
                        if (!datesEl) {
                            // Try finding in parent item for nested positions
                            const parentItem = item.closest('li.artdeco-list__item, li');
                            if (parentItem && parentItem !== item) {
                                const parentEntity = parentItem.querySelector('[data-view-name="profile-component-entity"]') || parentItem;
                                if (parentEntity) {
                                    datesEl = parentEntity.querySelector('.pvs-entity__caption-wrapper[aria-hidden="true"], [class*="caption"][aria-hidden="true"]');
                                }
                            }
                        }
                        
                        // Extract dates from text if element found
                        if (datesEl) {
                            const datesText = datesEl.textContent.trim();
                            exp.dates = datesText;
                            
                            // Parse various date formats
                            // Format 1: "Jan 2024 - Present · 2 yrs 1 mo"
                            const dateMatch = datesText.match(/^(.+?)\\s*[-–]\\s*(.+?)\\s*·\\s*(.+)$/);
                            if (dateMatch) {
                                exp.start_date = dateMatch[1].trim();
                                exp.end_date = dateMatch[2].trim();
                                exp.duration = dateMatch[3].trim();
                            } else {
                                // Format 2: "Jan 2024 - Present" or "Jan 2024 to Present"
                                const simpleDateMatch = datesText.match(/^(.+?)\\s*[-–to]\\s*(.+)$/i);
                                if (simpleDateMatch) {
                                    exp.start_date = simpleDateMatch[1].trim();
                                    const endPart = simpleDateMatch[2].trim();
                                    // Check if duration is included
                                    if (endPart.includes('·')) {
                                        const endParts = endPart.split('·').map(p => p.trim());
                                        exp.end_date = endParts[0];
                                        exp.duration = endParts[1] || 'N/A';
                                    } else {
                                        exp.end_date = endPart;
                                    }
                                } else {
                                    // Format 3: Just duration like "49 yrs 1 mo"
                                    const durationMatch = datesText.match(/(\\d+\\s*(?:yrs?|mos?|yr|mo)\\s*(?:\\d+\\s*(?:yrs?|mos?|yr|mo))?)/i);
                                    if (durationMatch) {
                                        exp.duration = durationMatch[1].trim();
                                    } else {
                                        // Format 4: Try to extract start_date from the full text
                                        const startMatch = datesText.match(/([A-Za-z]{3}\\s+\\d{4}|\\d{4})/);
                                        if (startMatch) {
                                            exp.start_date = startMatch[1];
                                        }
                                    }
                                }
                            }
                        } else {
                            // Try to extract dates from text content (for simpler structures)
                            const allSpans = entityDiv ? entityDiv.querySelectorAll('span[aria-hidden="true"], span') : item.querySelectorAll('span[aria-hidden="true"], span');
                            for (const span of allSpans) {
                                const text = span.textContent.trim();
                                // Look for date patterns
                                if (text.includes(' - ') || text.includes(' to ') || text.match(/\\d{4}/)) {
                                    const dateMatch = text.match(/(.+?)\\s*[-–to]\\s*(.+?)(?:\\s*·\\s*(.+))?/i);
                                    if (dateMatch) {
                                        exp.dates = text;
                                        exp.start_date = dateMatch[1].trim();
                                        exp.end_date = dateMatch[2].trim();
                                        if (dateMatch[3]) {
                                            exp.duration = dateMatch[3].trim();
                                        }
                                        break;
                                    }
                                }
                                // Also check for duration patterns
                                if (text.match(/\\d+\\s*(?:yrs?|mos?)/i) && !exp.duration || exp.duration === 'N/A') {
                                    const durationMatch = text.match(/(\\d+\\s*(?:yrs?|mos?)\\s*(?:\\d+\\s*(?:yrs?|mos?))?)/i);
                                    if (durationMatch) {
                                        exp.duration = durationMatch[1].trim();
                                    }
                                }
                            }
                            
                            // Last resort: extract from full item text
                            if (exp.dates === 'N/A' && itemText) {
                                const datePattern = /([A-Za-z]{3}\\s+\\d{4}|\\d{4})\\s*[-–to]\\s*([A-Za-z]{3}\\s+\\d{4}|\\d{4}|Present|present)/i;
                                const match = itemText.match(datePattern);
                                if (match) {
                                    exp.start_date = match[1].trim();
                                    exp.end_date = match[2].trim();
                                    exp.dates = `${exp.start_date} - ${exp.end_date}`;
                                }
                            }
                        }
                        
                        // Extract location and work type - ULTRA-UNIVERSAL method
                        if (entityDiv) {
                            const locationSpans = entityDiv.querySelectorAll('span.t-14.t-normal.t-black--light > span[aria-hidden="true"], span[class*="light"] > span[aria-hidden="true"], span[aria-hidden="true"]');
                            for (const span of locationSpans) {
                                const text = span.textContent.trim();
                                // Skip if it's the dates (already captured) - check for date patterns
                                if (text.includes(' - ') || text.includes(' to ') || /\\d{4}/.test(text)) {
                                    // Might be dates, check if we already captured it
                                    if (exp.dates && !exp.dates.includes(text)) {
                                        // Could be alternate date format
                                        const altDateMatch = text.match(/([A-Za-z]{3}\\s+\\d{4}|\\d{4})\\s*(?:to|-)\\s*(.+)/);
                                        if (altDateMatch) {
                                            if (exp.start_date === 'N/A') exp.start_date = altDateMatch[1].trim();
                                            if (exp.end_date === 'N/A') exp.end_date = altDateMatch[2].trim();
                                        }
                                    }
                                    continue;
                                }
                                
                                // Skip if it's employment type
                                if (text.toLowerCase().includes('full-time') || 
                                    text.toLowerCase().includes('part-time') ||
                                    text.toLowerCase().includes('internship') ||
                                    text.toLowerCase().includes('contract') ||
                                    text.toLowerCase().includes('self-employed')) {
                                    continue;
                                }
                                
                                // This is likely location
                                if (text && text.length > 2 && text.length < 200 && exp.location === 'N/A') {
                                    exp.location = text;
                                    // Extract work type from location text
                                    if (text.includes('Remote') || text.toLowerCase().includes('remote')) {
                                        exp.work_type = 'Remote';
                                    } else if (text.includes('On-site') || text.toLowerCase().includes('on-site') || text.toLowerCase().includes('onsite')) {
                                        exp.work_type = 'On-site';
                                    } else if (text.includes('Hybrid') || text.toLowerCase().includes('hybrid')) {
                                        exp.work_type = 'Hybrid';
                                    }
                                    // Remove work type from location if it's separate
                                    exp.location = exp.location.replace(/·\\s*Remote/i, '').replace(/·\\s*On-site/i, '').replace(/·\\s*Hybrid/i, '').trim();
                                } else if (text && text.length > 2 && text.length < 200 && exp.location !== 'N/A' && !exp.location.includes(text) && !text.includes('·')) {
                                    // Additional location info
                                    exp.location += ' · ' + text;
                                }
                            }
                        }
                        
                        // Also try to extract location from full item text (for simpler structures)
                        if (exp.location === 'N/A' && itemText) {
                            // Look for location patterns (city, country, state patterns)
                            const locationPatterns = [
                                /([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*),\\s*([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)/, // "City, State" or "City, Country"
                                /([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)\\s*·\\s*(Remote|On-site|Hybrid)/i, // "Location · Remote"
                            ];
                            
                            for (const pattern of locationPatterns) {
                                const match = itemText.match(pattern);
                                if (match) {
                                    exp.location = match[1] || match[0];
                                    if (match[2] && (match[2].toLowerCase().includes('remote') || match[2].toLowerCase().includes('on-site') || match[2].toLowerCase().includes('hybrid'))) {
                                        exp.work_type = match[2];
                                    }
                                    break;
                                }
                            }
                        }
                        
                        // ULTRA-UNIVERSAL skills link detection - works for ALL profile types and structures
                        // Search in multiple locations with multiple methods
                        let skillsLink = null;
                        
                        // Method 1: Check by data-field attribute (most reliable LinkedIn pattern)
                        skillsLink = entityDiv.querySelector('a[data-field="position_contextual_skills_see_details"]');
                        
                        // Method 2: Check in nested sub-components (for nested positions)
                        if (!skillsLink) {
                            const subComponents = entityDiv.querySelector('.pvs-entity__sub-components, [class*="sub-components"]');
                            if (subComponents) {
                                skillsLink = subComponents.querySelector('a[data-field="position_contextual_skills_see_details"]');
                            }
                        }
                        
                        // Method 3: Check in the entire item (for some structures)
                        if (!skillsLink) {
                            skillsLink = item.querySelector('a[data-field="position_contextual_skills_see_details"]');
                        }
                        
                        // Method 4: Check by href pattern - be VERY aggressive
                        if (!skillsLink) {
                            const allLinks = item.querySelectorAll('a');
                            for (const link of allLinks) {
                                const href = (link.getAttribute('href') || '').trim();
                                // Check for ANY skill-related href pattern
                                if (href && (
                                    href.includes('skill-associations') || 
                                    href.includes('position_contextual_skills') ||
                                    href.includes('skill') ||
                                    (href.includes('/overlay/') && href.includes('/details/'))
                                )) {
                                    skillsLink = link;
                                    break;
                                }
                            }
                        }
                        
                        // Method 5: Check by text content - be VERY lenient
                        if (!skillsLink) {
                            const allLinks = item.querySelectorAll('a');
                            for (const link of allLinks) {
                                const text = (link.textContent || '').toLowerCase().trim();
                                const href = (link.getAttribute('href') || '').trim();
                                
                                // Check for ANY skill-related text pattern
                                if (text && (
                                    (text.includes('skill') && (text.includes('+') || text.includes('show') || text.includes('see') || text.match(/\\d+/))) ||
                                    (text.includes('show') && text.includes('skill')) ||
                                    (text.includes('see') && text.includes('skill')) ||
                                    text.match(/\\d+\\s+skill/i) ||
                                    text.match(/\\+\\d+\\s+skill/i)
                                )) {
                                    // Must have a valid href
                                    if (href && (href.includes('/details/') || href.includes('skill') || href.includes('/overlay/'))) {
                                        skillsLink = link;
                                        break;
                                    }
                                }
                            }
                        }
                        
                        // Method 6: Search in parent items (for nested positions)
                        if (!skillsLink) {
                            const parentItem = item.closest('li');
                            if (parentItem && parentItem !== item) {
                                skillsLink = parentItem.querySelector('a[data-field="position_contextual_skills_see_details"]');
                                if (!skillsLink) {
                                    const parentLinks = parentItem.querySelectorAll('a');
                                    for (const link of parentLinks) {
                                        const href = (link.getAttribute('href') || '').trim();
                                        if (href && (href.includes('skill-associations') || href.includes('position_contextual_skills'))) {
                                            skillsLink = link;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Method 7: Last resort - search entire item for ANY link with skill-related href
                        if (!skillsLink) {
                            const allLinksInItem = item.querySelectorAll('a');
                            for (const link of allLinksInItem) {
                                const href = (link.getAttribute('href') || '').trim();
                                if (href && (
                                    href.includes('skill') ||
                                    (href.includes('/overlay/') && href.includes('details'))
                                )) {
                                    // Verify it's not a company link or other non-skill link
                                    const text = (link.textContent || '').toLowerCase();
                                    if (!text.includes('company') && !text.includes('view profile')) {
                                        skillsLink = link;
                                        break;
                                    }
                                }
                            }
                        }
                        
                        if (skillsLink) {
                            // Store the href for later clicking
                            exp.skills_link_href = skillsLink.href || skillsLink.getAttribute('href');
                            // Try to extract preview skills from link text (e.g., "Facebook Marketing, Microsoft Office and +5 skills")
                            const linkText = skillsLink.textContent.trim();
                            if (linkText) {
                                // Extract visible skills from text like "Facebook Marketing, Microsoft Office and +5 skills"
                                const skillsMatch = linkText.match(/(.+?)(?:\s+and\s+\+\d+\s+skills)?$/);
                                if (skillsMatch) {
                                    const skillsText = skillsMatch[1].trim();
                                    const previewSkills = skillsText.split(',').map(s => s.trim()).filter(s => s && s.length > 0);
                                    if (previewSkills.length > 0) {
                                        exp.skills_preview = previewSkills;
                                    }
                                }
                            }
                        } else {
                            // Method 6: Check if skills are shown directly in experience details (like "Skills: CodeIgniter · Laravel")
                            // This happens on experience details pages where skills are inline
                            const itemText = item.textContent || '';
                            if (itemText.includes('Skills:') || itemText.includes('skills:')) {
                                // Try to extract skills from text pattern "Skills: Skill1 · Skill2 · Skill3"
                                const skillsMatch = itemText.match(/Skills?:\s*([^\\n]+)/i);
                                if (skillsMatch) {
                                    const skillsText = skillsMatch[1];
                                    const skills = skillsText.split('·').map(s => s.trim()).filter(s => s && s.length > 0 && s.length < 60);
                                    if (skills.length > 0) {
                                        exp.skills_preview = skills;
                                    }
                                }
                            }
                        }
                        
                        // Extract description from sub-components (but not skills)
                        const skillsEl = entityDiv.querySelector('.pvs-entity__sub-components');
                        if (skillsEl) {
                            // Look for description text (longer text that's not skills)
                            const descSpans = skillsEl.querySelectorAll('span[aria-hidden="true"]');
                            for (const span of descSpans) {
                                const text = span.textContent.trim();
                                // Skip skills-related text
                                if (text.includes('skills') || text.includes('Skills') || text.includes('+')) {
                                    continue;
                                }
                                // Look for longer description text
                                if (text && text.length > 30 && exp.description === 'N/A' && !text.includes('·')) {
                                    exp.description = text;
                                    break;
                                }
                            }
                            
                            // Also try to get description from nested containers
                            if (exp.description === 'N/A') {
                                const descContainers = skillsEl.querySelectorAll('.display-flex');
                                for (const container of descContainers) {
                                    const text = container.textContent.trim();
                                    // Skip if it contains skills link or button
                                    if (text.includes('skills') || container.querySelector('a[data-field="position_contextual_skills_see_details"]')) {
                                        continue;
                                    }
                                    if (text && text.length > 30 && !text.includes('·')) {
                                        exp.description = text;
                                        break;
                                    }
                                }
                            }
                        }
                        
                        // Get company LinkedIn URL
                        const companyLink = entityDiv.querySelector('a[href*="/company/"]');
                        if (companyLink) {
                            exp.company_url = companyLink.href;
                        }
                        
                        // FLEXIBLE VALIDATION: Accept experiences with company OR dates OR location OR valid title
                        // This handles all edge cases: company-as-title, missing dates, nested positions, etc.
                        const titleLower = (exp.title || '').toLowerCase();
                        const companyLower = (exp.company || '').toLowerCase();
                        const locationLower = (exp.location || '').toLowerCase();
                        const datesLower = (exp.dates || '').toLowerCase();
                        const itemText = item.textContent || '';
                        
                        // FILTER OUT PERSON NAMES - but allow if they have company/dates/location
                        const namePattern = /^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}$/;
                        const looksLikeName = namePattern.test(exp.title) && exp.title.length < 60;
                        
                        // FILTER OUT RANKING TEXT (e.g., "· 3rd", "· 2nd", "· 1st")
                        const hasRanking = companyLower.includes('· 3rd') || 
                                          companyLower.includes('· 2nd') || 
                                          companyLower.includes('· 1st') ||
                                          locationLower.includes('· 3rd') ||
                                          locationLower.includes('· 2nd') ||
                                          locationLower.includes('· 1st');
                        
                        // FILTER OUT EDUCATION ENTRIES
                        const isEducation = 
                            titleLower.includes('university') ||
                            titleLower.includes('college') ||
                            titleLower.includes('school') ||
                            titleLower.includes('institute') ||
                            companyLower.includes('bachelor') ||
                            companyLower.includes('master') ||
                            companyLower.includes('degree') ||
                            companyLower.includes('bs') ||
                            companyLower.includes('bsc') ||
                            companyLower.includes('msc') ||
                            companyLower.includes('ms') ||
                            companyLower.includes('hsc') ||
                            companyLower.includes('ssc') ||
                            /^[a-z]+\s*university/i.test(exp.title) ||
                            /^[a-z]+\s*college/i.test(exp.title) ||
                            /^[a-z]+\s*school/i.test(exp.title);
                        
                        // FILTER OUT SALES NAVIGATOR INSIGHTS AND OTHER NOISE
                        const isNoise = 
                            titleLower.includes('follows your company') ||
                            titleLower.includes('viewed your profile') ||
                            titleLower.includes('aware of your brand') ||
                            titleLower.includes('free insight') ||
                            titleLower.includes('sales navigator') ||
                            companyLower.includes('follows your company') ||
                            companyLower.includes('viewed your profile') ||
                            datesLower.includes('follows your company') ||
                            datesLower.includes('viewed your profile');
                        
                        // Check if this looks like a real experience - VERY FLEXIBLE
                        const hasCompany = exp.company && exp.company !== 'N/A' && !hasRanking;
                        const hasDates = exp.dates && exp.dates !== 'N/A' && !datesLower.includes('n/a');
                        const hasLocation = exp.location && exp.location !== 'N/A' && !hasRanking;
                        const hasTitle = exp.title && exp.title !== 'N/A' && exp.title !== 'Experience' && titleLower.length > 2;
                        
                        // Also check for duration indicators (like "49 yrs 1 mo", "5 yrs 1 mo") - indicates valid experience
                        const hasDuration = itemText.includes('yrs') || 
                                          itemText.includes('mos') || 
                                          itemText.includes('yr') ||
                                          itemText.includes('mo');
                        
                        // Check for employment type indicators
                        const hasEmploymentType = itemText.includes('Full-time') ||
                                                  itemText.includes('Part-time') ||
                                                  itemText.includes('Internship') ||
                                                  itemText.includes('Contract') ||
                                                  itemText.includes('Self-employed');
                        
                        // ULTRA-LENIENT VALIDATION: Accept if has ANY indicator of being an experience
                        // OR if it's on experience details page (trust the page context)
                        let isValidExperience = 
                            (isExperiencePage && (hasTitle || hasCompany || itemText.length > 20)) || // On experience page, be very lenient
                            ((hasCompany || hasDates || hasLocation || hasTitle || hasDuration || hasEmploymentType) &&
                            !isEducation &&
                            !isNoise &&
                            !hasRanking &&
                            (!looksLikeName || hasCompany || hasDates || hasLocation || hasDuration || hasEmploymentType));
                        
                        // Also accept if it has skills link (definitely an experience)
                        if (!isValidExperience && exp.skills_link_href) {
                            isValidExperience = true;
                        }
                        
                        // Also accept if it has description (definitely an experience)
                        if (!isValidExperience && exp.description && exp.description !== 'N/A' && exp.description.length > 30) {
                            isValidExperience = true;
                        }
                        
                        if (isValidExperience) {
                            experiences.push(exp);
                        }
                    }
                    
                    console.log(`[EXPERIENCE] Found ${experiences.length} experience entries`);
                    return experiences;
                }
            """)
            
            logger.info(f"[EXPERIENCE] Extracted {len(experiences)} experience entries from page")
            
            # Check if we're on experience details page - if so, stay there for skills extraction
            is_experience_details_page = '/details/experience' in page.url
            
            # IMPORTANT: Don't navigate back yet - we need to stay on experience details page for skills extraction
            # We'll navigate back after extracting all skills
            # Only navigate back if we navigated away AND we have no experiences (extraction failed)
            if show_all_navigated and not is_experience_details_page and len(experiences) == 0:
                try:
                    # Only go back if extraction failed
                    logger.debug("[EXPERIENCE] No experiences found, navigating back to try main profile extraction")
                    await asyncio.sleep(1)
                    await page.goto(original_url, wait_until='domcontentloaded', timeout=15000)
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.debug(f"Could not navigate back to profile: {e}")
            
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
            
            # Filter out invalid entries and deduplicate
            seen = set()
            unique_experiences = []
            for exp in experiences:
                # Clean up duplicate text patterns first
                if exp.get('title'):
                    exp['title'] = clean_duplicate_text(exp['title'])
                if exp.get('company'):
                    exp['company'] = clean_duplicate_text(exp['company'])
                
                # Get fields
                title = exp.get('title', '').strip()
                company = exp.get('company', '').strip()
                
                # Allow experiences with company name even if title is N/A (like "Mark Hughes" profile)
                # But skip if both title and company are invalid
                if (not title or title in ['Experience', 'N/A', '']) and (not company or company == 'N/A'):
                    continue
                
                title_lower = title.lower()
                company = exp.get('company', '').strip()
                company_lower = company.lower()
                location = exp.get('location', '').strip()
                location_lower = location.lower()
                dates = exp.get('dates', '').strip()
                dates_lower = dates.lower()
                
                # FILTER OUT PERSON NAMES - check if title looks like a person name
                name_pattern = re.match(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}$', title)
                looks_like_name = bool(name_pattern) and len(title) < 60
                
                # FILTER OUT RANKING TEXT (e.g., "· 3rd", "· 2nd", "· 1st")
                has_ranking = ('· 3rd' in company_lower or '· 2nd' in company_lower or '· 1st' in company_lower or
                              '· 3rd' in location_lower or '· 2nd' in location_lower or '· 1st' in location_lower)
                
                # FILTER OUT SALES NAVIGATOR INSIGHTS AND NOISE
                is_noise = (
                    'follows your company' in title_lower or
                    'viewed your profile' in title_lower or
                    'aware of your brand' in title_lower or
                    'free insight' in title_lower or
                    'sales navigator' in title_lower or
                    'follows your company' in company_lower or
                    'viewed your profile' in company_lower or
                    'follows your company' in dates_lower or
                    'viewed your profile' in dates_lower
                )
                
                # FILTER OUT EDUCATION ENTRIES - ENHANCED to avoid filtering employment at institutes
                # Only treat as education if it has clear education indicators like degree, GPA, field of study
                # AND does NOT have employment indicators like job titles or employment types
                has_education_indicators = (
                    'bachelor' in company_lower or
                    'master' in company_lower or
                    'degree' in company_lower or
                    'phd' in company_lower or
                    'diploma' in company_lower or
                    'gpa' in company_lower or
                    'honors' in company_lower or
                    'major' in company_lower or
                    'minor' in company_lower or
                    'graduated' in company_lower
                )
                
                has_employment_indicators = (
                    'developer' in title_lower or 'engineer' in title_lower or
                    'manager' in title_lower or 'designer' in title_lower or
                    'analyst' in title_lower or 'consultant' in title_lower or
                    'director' in title_lower or 'lead' in title_lower or
                    'specialist' in title_lower or 'coordinator' in title_lower or
                    'assistant' in title_lower or 'associate' in title_lower or
                    'executive' in title_lower or 'officer' in title_lower or
                    'trainer' in title_lower or 'trainner' in title_lower or
                    'instructor' in title_lower or 'mentor' in title_lower or
                    'full-time' in item_text or 'part-time' in item_text or
                    'internship' in item_text or 'contract' in item_text or
                    'freelance' in item_text or 'apprenticeship' in item_text
                )
                
                # Only consider it education if:
                # 1. It's clearly at a university/college/school AND has no employment indicators
                # 2. OR it has clear education degree indicators
                is_pure_school = (
                    ('university' in title_lower or 'college' in title_lower or 'school' in title_lower) and
                    not has_employment_indicators
                )
                
                is_education = (is_pure_school or has_education_indicators) and not has_employment_indicators
                
                # Check if this looks like a real experience - FLEXIBLE validation
                hasCompany = company and company != 'N/A' and not has_ranking
                hasDates = dates and dates != 'N/A' and 'n/a' not in dates_lower
                hasLocation = location and location != 'N/A' and not has_ranking
                hasTitle = title and title != 'N/A' and title != 'Experience' and len(title) > 2
                
                # Check for duration indicators (like "49 yrs 1 mo", "5 yrs 1 mo")
                item_text = f"{title} {company} {dates} {location}".lower()
                hasDuration = 'yrs' in item_text or 'mos' in item_text or 'yr' in item_text or 'mo' in item_text
                
                # Check for employment type indicators
                hasEmploymentType = ('full-time' in item_text or 
                                   'part-time' in item_text or 
                                   'internship' in item_text or 
                                   'contract' in item_text or 
                                   'self-employed' in item_text)
                
                # Must have at least ONE of: company, dates, location, title, duration, employment type
                # This handles all edge cases: company-as-title, missing dates, nested positions, etc.
                if not (hasCompany or hasDates or hasLocation or hasTitle or hasDuration or hasEmploymentType):
                    logger.debug(f"[EXPERIENCE] Skipping entry without any valid experience indicators: title={title}, company={company}")
                    continue
                
                if is_education:
                    logger.debug(f"[EXPERIENCE] Skipping education entry: {title}")
                    continue
                
                if is_noise:
                    logger.debug(f"[EXPERIENCE] Skipping noise entry: {title}")
                    continue
                
                if has_ranking:
                    logger.debug(f"[EXPERIENCE] Skipping entry with ranking: {title}")
                    continue
                
                if looks_like_name and not hasCompany:
                    logger.debug(f"[EXPERIENCE] Skipping person name without company: {title}")
                    continue
                
                # Create deduplication key
                key = f"{title}|{company}|{exp.get('start_date', 'N/A')}"
                if key not in seen:
                    seen.add(key)
                    unique_experiences.append(exp)
            
            # Store current page URL before extracting skills
            current_page_url = page.url
            
            # Now extract skills for each experience by clicking skills button
            # Check if we're on experience details page where skills might be shown directly
            # (is_experience_details_page was already set above)
            
            # ENHANCED: Navigate to experience details page if not already there to ensure all experiences are visible
            # This helps with skills extraction as all experiences are loaded on the details page
            if not is_experience_details_page and unique_experiences:
                try:
                    # Try to find and navigate to experience details page
                    exp_details_link = await page.evaluate("""
                        () => {
                            const allLinks = document.querySelectorAll('a');
                            for (const link of allLinks) {
                                const href = (link.getAttribute('href') || '').trim();
                                if (href && href.includes('/details/experience')) {
                                    return href;
                                }
                            }
                            return null;
                        }
                    """)
                    if exp_details_link:
                        if not exp_details_link.startswith('http'):
                            exp_details_link = f"https://www.linkedin.com{exp_details_link}"
                        logger.info(f"[EXPERIENCE] Navigating to experience details page for better skills extraction")
                        await page.goto(exp_details_link, wait_until='domcontentloaded', timeout=30000)
                        await asyncio.sleep(2)
                        is_experience_details_page = True
                        current_page_url = page.url
                except Exception as e:
                    logger.debug(f"Could not navigate to experience details page: {e}")
            
            logger.info(f"[EXPERIENCE] Found {len(unique_experiences)} experiences, extracting skills from modals...")
            for i, exp in enumerate(unique_experiences):
                skills_link_href = exp.get('skills_link_href')
                
                # First, check if skills are already in preview (extracted from inline text)
                if 'skills_preview' in exp and exp.get('skills_preview'):
                    # If we're on experience details page, try to get full skills list
                    # Otherwise, use preview skills
                    if not is_experience_details_page:
                        exp['skills'] = exp['skills_preview']
                        del exp['skills_preview']
                        if 'skills_link_href' in exp:
                            del exp['skills_link_href']
                        continue
                
                if skills_link_href:
                    try:
                        logger.debug(f"[EXPERIENCE] [{i+1}/{len(unique_experiences)}] Extracting skills for: {exp.get('title')} at {exp.get('company')}")
                        
                        # ENHANCED: Try to find and click the skills link on the current page using multiple methods
                        skills = []
                        try:
                            # Extract key part of href to match
                            href_key = skills_link_href.split('/')[-1].split('?')[0] if '/' in skills_link_href else skills_link_href
                            
                            # Look for the skills link in the current DOM using comprehensive search
                            exp_title = exp.get('title', '').strip()
                            exp_company = exp.get('company', '').strip()
                            
                            # ULTRA-UNIVERSAL: Use JavaScript to find skills link by ALL possible criteria
                            skills_link_info = await page.evaluate("""
                                (hrefKey, title, company) => {
                                    // Method 1: Find by data-field attribute (most reliable)
                                    let links = document.querySelectorAll('a[data-field="position_contextual_skills_see_details"]');
                                    for (const link of links) {
                                        const linkText = (link.textContent || '').toLowerCase();
                                        const href = (link.getAttribute('href') || '').trim();
                                        // Be lenient - if it has the data-field, it's likely a skills link
                                        if (href && (href.includes('skill') || href.includes('/overlay/') || href.includes('/details/'))) {
                                            return {
                                                found: true,
                                                href: href,
                                                method: 'data-field',
                                                text: linkText
                                            };
                                        }
                                    }
                                    
                                    // Method 2: Find by href pattern - be VERY aggressive
                                    const allLinks = document.querySelectorAll('a');
                                    for (const link of allLinks) {
                                        const href = (link.getAttribute('href') || '').trim();
                                        const linkText = (link.textContent || '').toLowerCase();
                                        
                                        // Check for ANY skill-related href pattern
                                        if (href && (
                                            href.includes('skill-associations') ||
                                            href.includes('position_contextual_skills') ||
                                            href.includes('skill') ||
                                            (href.includes('/overlay/') && href.includes('details')) ||
                                            (hrefKey && href.includes(hrefKey))
                                        )) {
                                            // If href matches skill pattern, it's likely a skills link
                                            // Be lenient with text verification
                                            if (linkText.includes('skill') || 
                                                linkText.includes('+') || 
                                                linkText.match(/\\d+\\s+skill/) ||
                                                linkText.length === 0 || // Empty text is OK if href matches
                                                href.includes('skill-associations')) {
                                                return {
                                                    found: true,
                                                    href: href,
                                                    method: 'href-pattern',
                                                    text: linkText
                                                };
                                            }
                                        }
                                    }
                                    
                                    // Method 3: Find by text pattern - be VERY lenient
                                    for (const link of allLinks) {
                                        const linkText = (link.textContent || '').trim().toLowerCase();
                                        const href = (link.getAttribute('href') || '').trim();
                                        
                                        // Check for ANY skills-related text pattern
                                        if (linkText && (
                                            (linkText.includes('skill') && (linkText.includes('+') || linkText.match(/\\d+\\s+skill/) || linkText.includes('and') || linkText.includes('show') || linkText.includes('see'))) ||
                                            (linkText.includes('show') && linkText.includes('skill')) ||
                                            (linkText.includes('see') && linkText.includes('skill')) ||
                                            linkText.match(/\\+\\d+\\s+skill/i) ||
                                            linkText.match(/\\d+\\s+skill/i)
                                        )) {
                                            // Must have a valid href
                                            if (href && (href.includes('/details/') || href.includes('skill') || href.includes('/overlay/'))) {
                                                return {
                                                    found: true,
                                                    href: href,
                                                    method: 'text-pattern',
                                                    text: linkText
                                                };
                                            }
                                        }
                                    }
                                    
                                    // Method 4: Last resort - find ANY link with skill-associations in href
                                    for (const link of allLinks) {
                                        const href = (link.getAttribute('href') || '').trim();
                                        if (href && href.includes('skill-associations')) {
                                            return {
                                                found: true,
                                                href: href,
                                                method: 'skill-associations-href',
                                                text: (link.textContent || '').trim().toLowerCase()
                                            };
                                        }
                                    }
                                    
                                    return {found: false};
                                }
                            """, href_key, exp_title, exp_company)
                            
                            skills_link = None
                            if skills_link_info.get('found'):
                                # Try to find the link element using ULTRA-UNIVERSAL methods
                                try:
                                    href_to_find = skills_link_info.get('href', '')
                                    if href_to_find:
                                        # Extract key parts of href for matching
                                        href_parts = href_to_find.split('/')
                                        last_part = href_parts[-1] if href_parts else ''
                                        second_last = href_parts[-2] if len(href_parts) > 1 else ''
                                        
                                        # Try multiple selectors - be VERY aggressive
                                        selectors = [
                                            f'a[data-field="position_contextual_skills_see_details"]',
                                            f'a[href*="skill-associations"]',
                                            f'a[href*="{href_key}"]' if href_key else None,
                                            f'a[href="{href_to_find}"]',
                                            f'a[href*="{last_part}"]' if last_part else None,
                                            f'a[href*="{second_last}"]' if second_last else None,
                                            f'a[href*="position_contextual_skills"]',
                                            f'a[href*="/overlay/"]',
                                        ]
                                        
                                        # Remove None values
                                        selectors = [s for s in selectors if s]
                                        
                                        for selector in selectors:
                                            try:
                                                all_links = await page.query_selector_all(selector)
                                                for link in all_links:
                                                    try:
                                                        link_href = await link.get_attribute('href')
                                                        if not link_href:
                                                            continue
                                                        
                                                        link_text = (await link.inner_text()).lower()
                                                        
                                                        # Match by href pattern - be lenient
                                                        href_matches = (
                                                            href_key in link_href if href_key else False
                                                        ) or (
                                                            'skill-associations' in link_href
                                                        ) or (
                                                            href_to_find in link_href or link_href in href_to_find
                                                        ) or (
                                                            last_part in link_href if last_part else False
                                                        ) or (
                                                            'position_contextual_skills' in link_href
                                                        )
                                                        
                                                        # Match by text - be lenient
                                                        text_matches = (
                                                            'skill' in link_text
                                                        ) or (
                                                            '+' in link_text and 'skill' in link_text
                                                        )
                                                        
                                                        # If href matches OR (href has skill pattern AND text suggests skill)
                                                        if href_matches or (('skill' in link_href or '/overlay/' in link_href) and (text_matches or len(link_text) == 0)):
                                                            skills_link = link
                                                            break
                                                    except Exception as e:
                                                        logger.debug(f"Error checking link: {e}")
                                                        continue
                                                if skills_link:
                                                    break
                                            except Exception as e:
                                                logger.debug(f"Error with selector {selector}: {e}")
                                                continue
                                        
                                        # If still not found, try JavaScript-based search
                                        if not skills_link:
                                            try:
                                                found_link = await page.evaluate("""
                                                    (targetHref) => {
                                                        const allLinks = document.querySelectorAll('a');
                                                        for (const link of allLinks) {
                                                            const href = (link.getAttribute('href') || '').trim();
                                                            if (href && (
                                                                href === targetHref ||
                                                                href.includes(targetHref.split('/').pop()) ||
                                                                (targetHref.includes('skill-associations') && href.includes('skill-associations'))
                                                            )) {
                                                                return true; // Found it
                                                            }
                                                        }
                                                        return false;
                                                    }
                                                """, href_to_find)
                                                
                                                if found_link:
                                                    # Use a more generic selector
                                                    skills_link = await page.query_selector(f'a[href*="skill-associations"]')
                                            except:
                                                pass
                                except Exception as e:
                                    logger.debug(f"Error finding skills link element: {e}")
                            
                            if skills_link:
                                # Click to open modal
                                try:
                                    await skills_link.scroll_into_view_if_needed()
                                    await asyncio.sleep(0.5)
                                    await skills_link.click()
                                    await asyncio.sleep(2.5)  # Wait for modal to open
                                    # Extract skills from modal
                                    skills = await self._extract_skills_from_open_modal(page)
                                    # Close modal
                                    try:
                                        await page.keyboard.press('Escape')
                                        await asyncio.sleep(0.5)
                                    except:
                                        # Try clicking close button
                                        try:
                                            close_btn = await page.query_selector('.artdeco-modal__dismiss, button[aria-label*="Close"], button[aria-label*="Dismiss"]')
                                            if close_btn:
                                                await close_btn.click()
                                                await asyncio.sleep(0.5)
                                        except:
                                            pass
                                except Exception as e:
                                    logger.debug(f"Error clicking skills link: {e}")
                            
                            # AGGRESSIVE FALLBACK: If we have href but couldn't find/click the link, try direct navigation
                            if not skills and skills_link_href:
                                try:
                                    # Try to find ANY skill button on the page that matches this experience
                                    all_skill_buttons = await page.query_selector_all('a[data-field="position_contextual_skills_see_details"], a[href*="skill-associations"]')
                                    for btn in all_skill_buttons:
                                        try:
                                            btn_href = await btn.get_attribute('href')
                                            if btn_href and (href_key in btn_href if href_key else False):
                                                await btn.scroll_into_view_if_needed()
                                                await asyncio.sleep(0.5)
                                                await btn.click()
                                                await asyncio.sleep(2.5)
                                                skills = await self._extract_skills_from_open_modal(page)
                                                try:
                                                    await page.keyboard.press('Escape')
                                                    await asyncio.sleep(0.5)
                                                except:
                                                    pass
                                                if skills:
                                                    break
                                        except:
                                            continue
                                except Exception as e:
                                    logger.debug(f"Error in aggressive skill button search: {e}")
                                    
                        except Exception as e:
                            logger.debug(f"Could not click skills link directly: {e}")
                        
                        # If clicking didn't work, navigate to URL
                        if not skills:
                            skills = await self._extract_skills_from_modal(page, skills_link_href, current_page_url)
                        
                        # If still no skills and we're on experience details page, try extracting from page directly
                        if not skills and is_experience_details_page:
                            try:
                                # Try to find skills shown directly on the page for this experience
                                exp_title = exp.get('title', '').strip().lower()
                                exp_company = exp.get('company', '').strip().lower()
                                
                                # AGGRESSIVE: Search for skills shown inline on experience details page
                                skills_from_page = await page.evaluate("""
                                    (title, company) => {
                                        const skills = [];
                                        
                                        // Method 1: Find all text that contains "Skills:" and extract skills
                                        const allElements = document.querySelectorAll('strong, span, div, p, li, h3, h4');
                                        for (const el of allElements) {
                                            const text = el.textContent || '';
                                            
                                            // Check for "Skills:" pattern
                                            if (text.includes('Skills:') || text.includes('skills:')) {
                                                // Extract skills after "Skills:"
                                                const skillsMatch = text.match(/Skills?:\\s*([^\\n]+)/i);
                                                if (skillsMatch) {
                                                    const skillsStr = skillsMatch[1];
                                                    // Split by · or comma
                                                    const skillArray = skillsStr.split(/[·,]/).map(s => s.trim()).filter(s => s && s.length > 0 && s.length <= 60);
                                                    skills.push(...skillArray);
                                                }
                                            }
                                            
                                            // Also check for patterns like "Skill1, Skill2 and +X skills"
                                            if (text.includes('skill') && (text.includes('+') || text.includes('and'))) {
                                                // Try to extract skills from text like "Communication, Management and +2 skills"
                                                const skillPattern = /([A-Za-z][^,]+(?:,\\s*[A-Za-z][^,]+)*)\\s+and\\s+\\+?\\d+\\s+skill/i;
                                                const match = text.match(skillPattern);
                                                if (match) {
                                                    const skillsStr = match[1];
                                                    const skillArray = skillsStr.split(',').map(s => s.trim()).filter(s => s && s.length > 0 && s.length <= 60);
                                                    skills.push(...skillArray);
                                                }
                                            }
                                        }
                                        
                                        // Method 2: Look for skills in list format (Skills: Skill1 · Skill2 · Skill3)
                                        const skillLists = document.querySelectorAll('ul, ol');
                                        for (const list of skillLists) {
                                            const listText = list.textContent || '';
                                            if (listText.includes('Skills:') || listText.includes('skills:')) {
                                                const items = list.querySelectorAll('li');
                                                for (const item of items) {
                                                    const itemText = item.textContent.trim();
                                                    if (itemText && itemText.length > 2 && itemText.length <= 60 && 
                                                        !itemText.toLowerCase().includes('skill') &&
                                                        !itemText.toLowerCase().includes('show') &&
                                                        !itemText.toLowerCase().includes('see')) {
                                                        skills.push(itemText);
                                                    }
                                                }
                                            }
                                        }
                                        
                                        return [...new Set(skills)]; // Deduplicate
                                    }
                                """, exp_title, exp_company)
                                
                                if skills_from_page:
                                    skills = self._validate_skills(skills_from_page)
                                    logger.debug(f"[EXPERIENCE] Extracted {len(skills)} skills directly from experience details page")
                            except Exception as e:
                                logger.debug(f"Could not extract skills from page: {e}")
                        
                        if skills:
                            exp['skills'] = skills
                            logger.debug(f"[EXPERIENCE] Extracted {len(skills)} skills: {skills}")
                        else:
                            # If modal extraction failed, use preview skills if available
                            if 'skills_preview' in exp:
                                exp['skills'] = exp['skills_preview']
                                logger.debug(f"[EXPERIENCE] Using preview skills: {exp['skills']}")
                        # Clean up temporary fields
                        if 'skills_link_href' in exp:
                            del exp['skills_link_href']
                        if 'skills_preview' in exp:
                            del exp['skills_preview']
                        # Small delay between skill extractions
                        await asyncio.sleep(1)
                    except Exception as e:
                        logger.warning(f"Failed to extract skills for experience: {e}")
                        # Use preview skills if available
                        if 'skills_preview' in exp:
                            exp['skills'] = exp['skills_preview']
                            del exp['skills_preview']
                        if 'skills_link_href' in exp:
                            del exp['skills_link_href']
                        # Try to return to correct page
                        try:
                            if current_page_url and page.url != current_page_url:
                                await page.goto(current_page_url, wait_until='domcontentloaded', timeout=10000)
                        except:
                            pass
                else:
                    # No skills link, use preview if available
                    if 'skills_preview' in exp:
                        exp['skills'] = exp['skills_preview']
                        del exp['skills_preview']
            
            logger.info(f"[EXPERIENCE] Extracted {len(unique_experiences)} unique experience entries with skills")
            
            # Navigate back to original profile page if we navigated away
            if show_all_navigated and original_url and page.url != original_url:
                try:
                    logger.debug("[EXPERIENCE] Navigating back to original profile page")
                    await page.goto(original_url, wait_until='domcontentloaded', timeout=15000)
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.debug(f"Could not navigate back to original profile: {e}")
            
            return unique_experiences
            
        except Exception as e:
            logger.warning(f"Error extracting experience: {e}")
            # Try to navigate back if we were on experience page
            if 'show_all_navigated' in locals() and show_all_navigated and original_url:
                try:
                    await page.goto(original_url, wait_until='domcontentloaded', timeout=10000)
                except:
                    pass
            return experiences
    
    async def _extract_skills_from_open_modal(self, page: Page) -> List[str]:
        """Extract skills from an already-open modal (no navigation needed)"""
        skills = []
        try:
            # Scroll through modal to load all skills
            try:
                modal_content = await page.query_selector('.artdeco-modal__content, .pvs-modal__content')
                if modal_content:
                    for _ in range(5):
                        await page.evaluate("""
                            () => {
                                const modal = document.querySelector('.artdeco-modal__content, .pvs-modal__content');
                                if (modal) {
                                    modal.scrollTop = modal.scrollHeight;
                                }
                            }
                        """)
                        await asyncio.sleep(0.5)
            except:
                pass
            
            # Extract skills from modal with better filtering - handle all modal structures
            skills_list = await page.evaluate("""
                () => {
                    const skills = [];
                    
                    // Method 1: More specific selector for skills in modal
                    const skillElements = document.querySelectorAll(
                        '.artdeco-modal [data-view-name="profile-component-entity"] .mr1.t-bold span[aria-hidden="true"], ' +
                        '.pvs-modal [data-view-name="profile-component-entity"] .mr1.t-bold span[aria-hidden="true"], ' +
                        '.artdeco-modal [data-view-name="profile-component-entity"] .t-bold span[aria-hidden="true"], ' +
                        '.pvs-modal [data-view-name="profile-component-entity"] .t-bold span[aria-hidden="true"]'
                    );
                    
                    for (const skillEl of skillElements) {
                        const skillName = skillEl.textContent.trim();
                        
                        // Basic validation
                        if (!skillName || skillName.length === 0 || skillName.length > 60) {
                            continue;
                        }
                        
                        // Skip non-skill text
                        const skipTexts = ['Learn more', 'Skills', 'discover', 'endorsed', 'endorsement', 
                                          'See all', 'Show all', 'people', 'connection', 'message', 'follow',
                                          'Learn more about these skills'];
                        if (skipTexts.some(s => skillName.toLowerCase().includes(s.toLowerCase()))) {
                            continue;
                        }
                        
                        skills.push(skillName);
                    }
                    
                    // Method 2: Also check for skills in list items (for modal structure)
                    if (skills.length === 0) {
                        const skillItems = document.querySelectorAll('.artdeco-modal li.artdeco-list__item, .pvs-modal li.artdeco-list__item');
                        for (const item of skillItems) {
                            const skillEntity = item.querySelector('[data-view-name="profile-component-entity"]');
                            if (skillEntity) {
                                const skillNameEl = skillEntity.querySelector('.mr1.t-bold span[aria-hidden="true"], .t-bold span[aria-hidden="true"]');
                                if (skillNameEl) {
                                    const skillName = skillNameEl.textContent.trim();
                                    if (skillName && skillName.length > 0 && skillName.length <= 60) {
                                        const skipTexts = ['Learn more', 'Skills', 'discover', 'endorsed', 'endorsement', 
                                                          'See all', 'Show all', 'people', 'connection', 'message', 'follow',
                                                          'Learn more about these skills'];
                                        if (!skipTexts.some(s => skillName.toLowerCase().includes(s.toLowerCase()))) {
                                            skills.push(skillName);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    // Method 3: Extract from "Skills:" text followed by skill names (for experience details page)
                    if (skills.length === 0) {
                        const skillsSection = document.querySelector('strong:has-text("Skills:")');
                        if (skillsSection) {
                            const parent = skillsSection.closest('div');
                            if (parent) {
                                const skillsText = parent.textContent;
                                // Extract skills after "Skills:" - format like "Skills: Skill1 · Skill2 · Skill3"
                                const skillsMatch = skillsText.match(/Skills:\\s*(.+)/);
                                if (skillsMatch) {
                                    const skillsStr = skillsMatch[1];
                                    const skillArray = skillsStr.split('·').map(s => s.trim()).filter(s => s && s.length > 0 && s.length <= 60);
                                    skills.push(...skillArray);
                                }
                            }
                        }
                    }
                    
                    return [...new Set(skills)]; // Deduplicate
                }
            """)
            
            if skills_list:
                # Additional Python-side validation to filter out person names
                skills = self._validate_skills(skills_list)
                
        except Exception as e:
            logger.debug(f"Error extracting skills from open modal: {e}")
        
        return skills
    
    def _validate_skills(self, skills: List[str]) -> List[str]:
        """Validate skills list - filter out person names and invalid entries"""
        valid_skills = []
        
        # Common skill keywords/patterns (technology, tools, concepts)
        skill_indicators = [
            'python', 'java', 'javascript', 'c++', 'c#', 'sql', 'html', 'css', 'react', 'angular',
            'node', 'django', 'flask', 'spring', 'aws', 'azure', 'docker', 'kubernetes', 'git',
            'machine learning', 'data', 'analytics', 'design', 'development', 'programming',
            'management', 'leadership', 'communication', 'project', 'agile', 'scrum',
            'marketing', 'sales', 'finance', 'accounting', 'engineering', 'testing',
            'api', 'web', 'mobile', 'cloud', 'database', 'security', 'network',
            'microsoft', 'office', 'excel', 'powerpoint', 'word', 'photoshop', 'illustrator',
            'tensorflow', 'pytorch', 'numpy', 'pandas', 'scikit', 'visualization',
            'research', 'analysis', 'problem', 'solving', 'team', 'collaboration',
            '.net', 'mvc', 'asp', 'entity', 'linq', 'solid', 'oop', 'rest', 'graphql',
            'bootstrap', 'jquery', 'typescript', 'vue', 'svelte', 'tailwind',
            'mysql', 'postgresql', 'mongodb', 'redis', 'elasticsearch', 'kafka',
            'ci/cd', 'devops', 'terraform', 'ansible', 'jenkins', 'github', 'gitlab',
            'linux', 'unix', 'windows', 'shell', 'bash', 'powershell',
            'jira', 'confluence', 'trello', 'slack', 'notion', 'asana',
            'figma', 'sketch', 'ui', 'ux', 'wireframe', 'prototype',
        ]
        
        for skill in skills:
            if not skill or len(skill) > 60:
                continue
            
            skill_lower = skill.lower()
            
            # Skip if it looks like a person name (typically 2-4 capitalized words)
            words = skill.split()
            if len(words) >= 2 and len(words) <= 4:
                # Check if all words are capitalized (likely a name)
                all_capitalized = all(w[0].isupper() and w[1:].islower() if len(w) > 1 else w[0].isupper() for w in words)
                # Check if it doesn't contain any skill indicators
                has_skill_indicator = any(ind in skill_lower for ind in skill_indicators)
                
                if all_capitalized and not has_skill_indicator:
                    # Likely a person name - skip it
                    continue
            
            # Skip obvious non-skills
            skip_patterns = ['followers', 'following', 'connections', 'mutual', 'degree', 
                           'endorsed by', 'people', 'endorsement', 'message', 'connect',
                           'see all', 'show all', 'learn more']
            if any(skip in skill_lower for skip in skip_patterns):
                continue
            
            valid_skills.append(skill)
        
        return valid_skills
    
    async def _extract_skills_from_modal(self, page: Page, skills_url: str, return_url: str = None) -> List[str]:
        """Extract all skills from the skills modal/overlay for an experience"""
        skills = []
        original_url = page.url
        try:
            # Navigate to skills modal URL
            logger.debug(f"[SKILLS] Opening skills modal: {skills_url}")
            if not skills_url.startswith('http'):
                skills_url = f"https://www.linkedin.com{skills_url}"
            
            # Check if this is a modal overlay URL or a navigation URL
            # Modal overlays can be opened via JavaScript click or navigation
            if 'overlay' in skills_url:
                # This is likely a modal - try clicking the link instead of navigating
                # First, try to find and click the link on current page
                try:
                    skills_link = await page.query_selector(f'a[href*="{skills_url.split("/")[-1]}"], a[href="{skills_url}"]')
                    if skills_link:
                        await skills_link.click()
                        await asyncio.sleep(2)  # Wait for modal to open
                    else:
                        # Fallback: navigate to URL
                        await page.goto(skills_url, wait_until='domcontentloaded', timeout=20000)
                        await asyncio.sleep(2)
                except:
                    # Fallback: navigate to URL
                    await page.goto(skills_url, wait_until='domcontentloaded', timeout=20000)
                    await asyncio.sleep(2)
            else:
                # Navigate to the URL
                await page.goto(skills_url, wait_until='domcontentloaded', timeout=20000)
                await asyncio.sleep(2)  # Wait for page content to load
            
            # Scroll through modal/page to load all skills (if lazy-loaded)
            try:
                # Check if we're in a modal or on a full page
                modal_content = await page.query_selector('.artdeco-modal__content, .pvs-modal__content')
                if modal_content:
                    # Scroll within modal to trigger lazy loading
                    for _ in range(5):  # More scrolls to ensure all skills load
                        await page.evaluate("""
                            () => {
                                const modal = document.querySelector('.artdeco-modal__content, .pvs-modal__content');
                                if (modal) {
                                    modal.scrollTop = modal.scrollHeight;
                                }
                                // Also try scrolling the main content
                                const mainContent = document.querySelector('.pvs-list__container');
                                if (mainContent) {
                                    mainContent.scrollTop = mainContent.scrollHeight;
                                }
                            }
                        """)
                        await asyncio.sleep(0.5)
                else:
                    # On a full page, scroll the main content
                    for _ in range(3):
                        await page.evaluate("""
                            () => {
                                const content = document.querySelector('.scaffold-finite-scroll__content, .pvs-list__container');
                                if (content) {
                                    content.scrollTop = content.scrollHeight;
                                }
                                window.scrollTo(0, document.body.scrollHeight);
                            }
                        """)
                        await asyncio.sleep(0.5)
            except:
                pass
            
            # Extract all skills from modal/page - comprehensive extraction
            skills_list = await page.evaluate("""
                () => {
                    const skills = [];
                    
                    // Method 1: More specific selector for skills in modal/page
                    const skillElements = document.querySelectorAll(
                        '[data-view-name="profile-component-entity"] .mr1.t-bold span[aria-hidden="true"], ' +
                        '[data-view-name="profile-component-entity"] .t-bold span[aria-hidden="true"]'
                    );
                    
                    for (const skillEl of skillElements) {
                        const skillName = skillEl.textContent.trim();
                        
                        // Basic validation
                        if (!skillName || skillName.length === 0 || skillName.length > 60) {
                            continue;
                        }
                        
                        // Skip non-skill text
                        const skipTexts = ['Learn more', 'Skills', 'discover', 'endorsed', 'endorsement', 
                                          'See all', 'Show all', 'people', 'connection', 'message', 'follow',
                                          'Learn more about these skills'];
                        if (skipTexts.some(s => skillName.toLowerCase().includes(s.toLowerCase()))) {
                            continue;
                        }
                        
                        skills.push(skillName);
                    }
                    
                    // Method 2: Also check for skills in list items (for modal/page structure)
                    if (skills.length === 0) {
                        const skillItems = document.querySelectorAll('li.artdeco-list__item');
                        for (const item of skillItems) {
                            const skillEntity = item.querySelector('[data-view-name="profile-component-entity"]');
                            if (skillEntity) {
                                const skillNameEl = skillEntity.querySelector('.mr1.t-bold span[aria-hidden="true"], .t-bold span[aria-hidden="true"]');
                                if (skillNameEl) {
                                    const skillName = skillNameEl.textContent.trim();
                                    if (skillName && skillName.length > 0 && skillName.length <= 60) {
                                        const skipTexts = ['Learn more', 'Skills', 'discover', 'endorsed', 'endorsement', 
                                                          'See all', 'Show all', 'people', 'connection', 'message', 'follow',
                                                          'Learn more about these skills'];
                                        if (!skipTexts.some(s => skillName.toLowerCase().includes(s.toLowerCase()))) {
                                            skills.push(skillName);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    // Method 3: Extract from "Skills:" text followed by skill names (for experience details page)
                    if (skills.length === 0) {
                        // Search for "Skills:" text in various elements
                        const allElements = document.querySelectorAll('strong, span, div, p');
                        for (const el of allElements) {
                            const text = el.textContent || '';
                            if (text.includes('Skills:') || text.includes('skills:')) {
                                // Extract skills after "Skills:" - format like "Skills: Skill1 · Skill2 · Skill3"
                                const skillsMatch = text.match(/Skills?:\s*([^\\n]+)/i);
                                if (skillsMatch) {
                                    const skillsStr = skillsMatch[1];
                                    const skillArray = skillsStr.split('·').map(s => s.trim()).filter(s => s && s.length > 0 && s.length <= 60);
                                    skills.push(...skillArray);
                                    break; // Found skills, no need to continue
                                }
                            }
                        }
                        
                        // Also check parent containers if not found
                        if (skills.length === 0) {
                            const allStrong = document.querySelectorAll('strong');
                            for (const strong of allStrong) {
                                if (strong.textContent.includes('Skills:') || strong.textContent.includes('skills:')) {
                                    const parent = strong.closest('div, section, li');
                                    if (parent) {
                                        const skillsText = parent.textContent;
                                        const skillsMatch = skillsText.match(/Skills?:\s*([^\\n]+)/i);
                                        if (skillsMatch) {
                                            const skillsStr = skillsMatch[1];
                                            const skillArray = skillsStr.split('·').map(s => s.trim()).filter(s => s && s.length > 0 && s.length <= 60);
                                            skills.push(...skillArray);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    return [...new Set(skills)]; // Deduplicate
                }
            """)
            
            if skills_list and len(skills_list) > 0:
                # Validate skills to filter out person names
                skills = self._validate_skills(skills_list)
                logger.debug(f"[SKILLS] Extracted {len(skills)} skills from modal")
            
            # Close modal and return to previous page
            try:
                # Try pressing ESC to close modal
                await page.keyboard.press('Escape')
                await asyncio.sleep(0.5)
                
                # Navigate back to the experience page (either go_back or use return_url)
                current_url = page.url
                if 'overlay' in current_url or 'skill-associations' in current_url:
                    if return_url:
                        # Navigate directly to return URL
                        await page.goto(return_url, wait_until='domcontentloaded', timeout=10000)
                    else:
                        # Use browser back
                        await page.go_back(wait_until='domcontentloaded', timeout=10000)
                    await asyncio.sleep(1)
            except Exception as e:
                logger.debug(f"Error closing modal: {e}")
                # If ESC doesn't work, try clicking close button
                try:
                    close_btn = await page.query_selector('button[aria-label*="Close"], button[aria-label*="Dismiss"], .artdeco-modal__dismiss')
                    if close_btn:
                        await close_btn.click()
                        await asyncio.sleep(0.5)
                        if return_url:
                            await page.goto(return_url, wait_until='domcontentloaded', timeout=10000)
                except:
                    # Last resort: navigate to return_url or go back
                    try:
                        if return_url:
                            await page.goto(return_url, wait_until='domcontentloaded', timeout=10000)
                        else:
                            await page.go_back(wait_until='domcontentloaded', timeout=10000)
                    except:
                        pass
            
            return skills
            
        except Exception as e:
            logger.warning(f"Error extracting skills from modal: {e}")
            # Try to close modal/navigate back to return_url
            try:
                await page.keyboard.press('Escape')
                await asyncio.sleep(0.5)
                if return_url:
                    await page.goto(return_url, wait_until='domcontentloaded', timeout=10000)
                elif 'overlay' in page.url:
                    await page.go_back(wait_until='domcontentloaded', timeout=5000)
            except:
                pass
            return skills
    
    async def _extract_education(self, page: Page, all_text: str) -> List[Dict]:
        """Extract education entries with full structured data from LinkedIn profile"""
        education = []
        original_url = page.url
        try:
            # ========== AGENT-STYLE APPROACH (PRIMARY) ==========
            # Use the scrape_agent's universal education extraction if available
            # This handles: Show all education, Skills buttons, See more buttons
            if hasattr(self, 'scrape_agent') and self.scrape_agent:
                try:
                    logger.info("[EDUCATION] Using agent-style extraction for universal compatibility")
                    agent_education = await self.scrape_agent.extract_all_education_agent(original_url)
                    if agent_education and len(agent_education) > 0:
                        logger.info(f"[EDUCATION] Agent extracted {len(agent_education)} education entries successfully")
                        return agent_education
                    else:
                        logger.debug("[EDUCATION] Agent returned no education, falling back to standard extraction")
                except Exception as e:
                    logger.warning(f"[EDUCATION] Agent extraction failed, falling back: {e}")
            
            # ========== FALLBACK: Standard extraction method ==========
            # First, check if "Show all education" button exists and navigate to full education page
            show_all_navigated = False
            try:
                # Try finding "Show all education" button by exact ID first (most reliable)
                show_all_btn = None
                try:
                    # Check for exact ID selector first
                    show_all_btn = await page.query_selector('a[id="navigation-index-see-all-education"]')
                except:
                    pass
                
                # Also try finding by attribute contains
                if not show_all_btn:
                    try:
                        all_links = await page.query_selector_all('a')
                        for link in all_links:
                            try:
                                link_id = await link.get_attribute('id')
                                link_text = await link.inner_text()
                                href = await link.get_attribute('href')
                                
                                # Check if it's a "Show all education" button
                                if link_id and 'navigation-index-see-all-education' in link_id:
                                    show_all_btn = link
                                    break
                                # Also check by href and text
                                if href and '/details/education' in href and ('Show all' in link_text or 'education' in link_text.lower()):
                                    show_all_btn = link
                                    break
                            except:
                                continue
                    except:
                        pass
                
                if show_all_btn:
                    href = await show_all_btn.get_attribute('href')
                    if href:
                        logger.info(f"[EDUCATION] Found 'Show all education' button, navigating to full education page")
                        # Navigate to full education page
                        if not href.startswith('http'):
                            href = f"https://www.linkedin.com{href}"
                        await page.goto(href, wait_until='domcontentloaded', timeout=30000)
                        await asyncio.sleep(3)  # Wait for page load
                        
                        # Scroll to load all education items (lazy loading)
                        logger.debug("[EDUCATION] Scrolling to load all education items...")
                        for scroll_attempt in range(5):
                            await page.evaluate("""
                                () => {
                                    window.scrollTo(0, document.body.scrollHeight);
                                    const content = document.querySelector('.scaffold-finite-scroll__content, .pvs-list__container');
                                    if (content) {
                                        content.scrollTop = content.scrollHeight;
                                    }
                                }
                            """)
                            await asyncio.sleep(1)
                            
                            # Check if "Show more results" button exists and click it
                            try:
                                show_more_btn = await page.query_selector('button.scaffold-finite-scroll__load-button, button[aria-label*="Show more"]')
                                if show_more_btn:
                                    is_visible = await show_more_btn.is_visible()
                                    if is_visible:
                                        await show_more_btn.scroll_into_view_if_needed()
                                        await show_more_btn.click()
                                        await asyncio.sleep(2)
                                        logger.debug(f"[EDUCATION] Clicked 'Show more results' button (attempt {scroll_attempt + 1})")
                                    else:
                                        break
                                else:
                                    break
                            except Exception as e:
                                logger.debug(f"[EDUCATION] Error checking for 'Show more' button: {e}")
                                pass
                        
                        show_all_navigated = True
            except Exception as e:
                logger.debug(f"Could not navigate to show all education: {e}")
            
            # Extract education using JavaScript from education detail page or main profile
            education = await page.evaluate("""
                () => {
                    const education = [];
                    
                    // Find all education items - works on both main profile and details page
                    // First, identify if we're in an education section/page
                    const isEducationPage = window.location.href.includes('/details/education');
                    
                    // Get education section (avoid experience section and other sections)
                    let eduSection = null;
                    const allSections = document.querySelectorAll('section');
                    for (const section of allSections) {
                        const sectionId = section.querySelector('#education');
                        const sectionHeader = section.querySelector('h2');
                        if (sectionId || (sectionHeader && sectionHeader.textContent.trim().toLowerCase() === 'education')) {
                            const expId = section.querySelector('#experience');
                            // Make sure it's not experience, following, interests, or other sections
                            const headerText = sectionHeader ? sectionHeader.textContent.trim().toLowerCase() : '';
                            if (!expId && 
                                headerText === 'education' && 
                                !headerText.includes('following') &&
                                !headerText.includes('interests') &&
                                !headerText.includes('skills')) {
                                eduSection = section;
                                break;
                            }
                        }
                    }
                    
                    // Get items from education section or all items if on education details page
                    let eduItems;
                    if (isEducationPage) {
                        eduItems = document.querySelectorAll('li.pvs-list__paged-list-item');
                    } else if (eduSection) {
                        eduItems = eduSection.querySelectorAll('li.pvs-list__paged-list-item, li.artdeco-list__item');
                    } else {
                        eduItems = document.querySelectorAll('li.pvs-list__paged-list-item, li.artdeco-list__item');
                    }
                    
                    for (const item of eduItems) {
                        // STRICT: Make sure we're in the education section
                        const parentSection = item.closest('section');
                        if (!parentSection) continue;
                        
                        // Check section ID
                        const eduId = parentSection.querySelector('#education');
                        const expId = parentSection.querySelector('#experience');
                        if (expId) continue;  // Skip if in experience section
                        
                        // Check section header - must be "Education" exactly
                        // But if we have eduId, trust it (section ID is more reliable)
                        if (!eduId) {
                            const sectionHeader = parentSection.querySelector('h2');
                            if (sectionHeader) {
                                const headerText = sectionHeader.textContent.trim().toLowerCase();
                                // Must be education section, not experience, connections, following, etc.
                                if (!headerText.includes('education') || 
                                    headerText.includes('experience') ||
                                    headerText.includes('connection') ||
                                    headerText.includes('following') ||
                                    headerText.includes('people') ||
                                    headerText.includes('recommendation') ||
                                    headerText.includes('interests') ||
                                    (headerText.includes('skills') && !headerText.includes('education'))) {
                                    continue;
                                }
                            } else {
                                // No header and no education ID - skip
                                continue;
                            }
                        }
                        
                        // Check if this is an education item by looking for profile-component-entity
                        const entityDiv = item.querySelector('[data-view-name="profile-component-entity"]');
                        if (!entityDiv) continue;
                        
                        // STRICT: Check if this looks like a connection/recommendation item
                        // Connections/recommendations often have person names as school names
                        const itemText = item.textContent || '';
                        // Skip if it looks like a person name pattern (2-3 words, capitalized, no school info)
                        const schoolCandidate = entityDiv.querySelector('.t-bold span[aria-hidden="true"]');
                        if (schoolCandidate) {
                            const schoolText = schoolCandidate.textContent.trim();
                            // Check if it's a person name (pattern: FirstName LastName or FirstName MiddleName LastName)
                            const namePattern = /^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}$/;
                            if (namePattern.test(schoolText) && schoolText.length < 50) {
                                // Check if there's no degree/school info nearby - if no degree, it's likely a person name
                                const hasDegreeInfo = itemText.includes('Bachelor') || 
                                                     itemText.includes('Master') ||
                                                     itemText.includes('BS') ||
                                                     itemText.includes('BSc') ||
                                                     itemText.includes('MS') ||
                                                     itemText.includes('MSc') ||
                                                     itemText.includes('Degree') ||
                                                     itemText.includes('University') ||
                                                     itemText.includes('College') ||
                                                     /Grade:/.test(itemText) ||
                                                     /\\d{4}/.test(itemText);  // Has year
                                if (!hasDegreeInfo) {
                                    continue;  // Skip person names without degree/school info
                                }
                            }
                        }
                        
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
                            activities: []
                        };
                        
                        // Extract school name (t-bold class, first one)
                        let schoolEl = entityDiv.querySelector('.t-bold span[aria-hidden="true"]');
                        if (!schoolEl) {
                            schoolEl = entityDiv.querySelector('.mr1.hoverable-link-text.t-bold span[aria-hidden="true"]');
                        }
                        if (schoolEl) {
                            edu.school = schoolEl.textContent.trim();
                        }
                        
                        // Extract degree/field of study from span.t-14.t-normal
                        const degreeSpans = entityDiv.querySelectorAll('span.t-14.t-normal > span[aria-hidden="true"]');
                        if (degreeSpans.length > 0) {
                            const degreeText = degreeSpans[0].textContent.trim();
                            edu.degree = degreeText || 'N/A';
                            // Sometimes field of study is separate
                            if (degreeSpans.length > 1) {
                                edu.field_of_study = degreeSpans[1].textContent.trim();
                            }
                        }
                        
                        // Extract dates and duration from pvs-entity__caption-wrapper
                        const datesEl = entityDiv.querySelector('.pvs-entity__caption-wrapper[aria-hidden="true"]');
                        if (datesEl) {
                            const datesText = datesEl.textContent.trim();
                            edu.dates = datesText;
                            
                            // Parse "Jan 2023 - Dec 2026" or "2021" format
                            const dateMatch = datesText.match(/^(.+?)\\s*[-–]\\s*(.+)$/);
                            if (dateMatch) {
                                edu.start_date = dateMatch[1].trim();
                                edu.end_date = dateMatch[2].trim();
                            } else {
                                // Single year format "2021"
                                const yearMatch = datesText.match(/^(\d{4})$/);
                                if (yearMatch) {
                                    edu.start_date = yearMatch[1];
                                    edu.end_date = yearMatch[1];
                                } else {
                                    // Try to extract start_date from the full text
                                    const startMatch = datesText.match(/^([A-Za-z]{3}\\s+\\d{4})/);
                                    if (startMatch) {
                                        edu.start_date = startMatch[1];
                                    }
                                }
                            }
                        }
                        
                        // Extract grade/description from sub-components
                        const subComponents = entityDiv.querySelector('.pvs-entity__sub-components');
                        if (subComponents) {
                            const gradeSpans = subComponents.querySelectorAll('span[aria-hidden="true"]');
                            for (const span of gradeSpans) {
                                const text = span.textContent.trim();
                                // Look for grade pattern like "Grade: 5.0/5.0 GPA (A+)"
                                if (text.toLowerCase().includes('grade') || text.toLowerCase().includes('gpa')) {
                                    edu.grade = text;
                                } else if (text && text.length > 20 && !edu.description.includes(text)) {
                                    // Description text
                                    if (edu.description === 'N/A') {
                                        edu.description = text;
                                    } else {
                                        edu.description += ' ' + text;
                                    }
                                }
                            }
                        }
                        
                        // Get school LinkedIn URL
                        const schoolLink = entityDiv.querySelector('a[href*="/company/"]');
                        if (schoolLink) {
                            edu.school_url = schoolLink.href;
                        }
                        
                        // STRICT VALIDATION: Only add if we have valid education data
                        if (edu.school && edu.school !== 'N/A' && edu.school !== 'Education') {
                            const schoolLower = edu.school.toLowerCase();
                            const degreeLower = (edu.degree || '').toLowerCase();
                            const datesLower = (edu.dates || '').toLowerCase();
                            const descriptionLower = (edu.description || '').toLowerCase();
                            
                            // FILTER OUT SALES NAVIGATOR INSIGHTS
                            const isSalesNavigator = 
                                schoolLower.includes('follows your company') ||
                                schoolLower.includes('viewed your profile') ||
                                schoolLower.includes('aware of your brand') ||
                                schoolLower.includes('free insight') ||
                                schoolLower.includes('sales navigator') ||
                                degreeLower.includes('follows your company') ||
                                degreeLower.includes('viewed your profile') ||
                                degreeLower.includes('aware of your brand') ||
                                degreeLower.includes('free insight') ||
                                degreeLower.includes('sales navigator') ||
                                descriptionLower.includes('free insight') ||
                                descriptionLower.includes('sales navigator');
                            
                            // FILTER OUT PERSON NAMES - check if school looks like a person name
                            const namePattern = /^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}$/;
                            const looksLikeName = namePattern.test(edu.school) && edu.school.length < 60;
                            
                            // FILTER OUT NOISE - companies, people, follower counts, etc.
                            const isNoise = 
                                // Sales Navigator insights
                                isSalesNavigator ||
                                // Follower counts (e.g., "1,571,704 followers", "3,294,337 followers")
                                /\\d+[,\\s]*\\d+[,\\s]*\\d+\\s+followers/i.test(edu.school) ||
                                /\\d+[,\\s]*\\d+[,\\s]*\\d+\\s+followers/i.test(edu.degree) ||
                                /\\d+[,\\s]*\\d+[,\\s]*\\d+\\s+followers/i.test(edu.dates) ||
                                // Member counts (e.g., "282,211 members")
                                /\\d+[,\\s]*\\d+[,\\s]*\\d+\\s+members/i.test(edu.school) ||
                                /\\d+[,\\s]*\\d+[,\\s]*\\d+\\s+members/i.test(edu.degree) ||
                                /\\d+[,\\s]*\\d+[,\\s]*\\d+\\s+members/i.test(edu.dates) ||
                                // Common company names (not schools)
                                schoolLower === 'fiverr' ||
                                schoolLower === 'upwork' ||
                                schoolLower === 'freelancer' ||
                                schoolLower === 'peopleperhour' ||
                                schoolLower.includes('fiverr') ||
                                schoolLower.includes('upwork') ||
                                schoolLower.includes('freelancer') ||
                                // Generic skill/course names (not schools)
                                schoolLower === 'logo design' ||
                                schoolLower === 'creative thinking' ||
                                (schoolLower.includes('design') && schoolLower.length < 20) ||
                                schoolLower.includes('thinking') ||
                                // Newsletter/company names
                                schoolLower.includes('toolbox') ||
                                schoolLower.includes('newsletter') ||
                                (schoolLower.includes('creator') && schoolLower.includes('toolbox')) ||
                                // Person names (unless they have degree/school indicators)
                                (looksLikeName && 
                                 !schoolLower.includes('university') && 
                                 !schoolLower.includes('college') && 
                                 !schoolLower.includes('school') && 
                                 !schoolLower.includes('institute') &&
                                 !degreeLower.includes('bachelor') &&
                                 !degreeLower.includes('master') &&
                                 !degreeLower.includes('degree') &&
                                 !degreeLower.includes('bs') &&
                                 !degreeLower.includes('bsc') &&
                                 !degreeLower.includes('ms') &&
                                 !degreeLower.includes('msc') &&
                                 !/Grade:/.test(edu.degree) &&
                                 !/\\d{4}/.test(edu.dates)) ||
                                // Ranking text (e.g., "· 3rd")
                                degreeLower === '· 3rd' ||
                                degreeLower === '· 2nd' ||
                                degreeLower === '· 1st' ||
                                degreeLower.startsWith('· ') ||
                                // Published/company info
                                datesLower.includes('published') ||
                                datesLower.includes('weekly') ||
                                datesLower.includes('followers') ||
                                datesLower.includes('members') ||
                                // Company URLs in school field
                                edu.school.includes('linkedin.com/company') ||
                                // Very short names that are likely not schools (unless they're abbreviations)
                                (edu.school.length < 5 && !/^[A-Z]{2,5}$/.test(edu.school));
                            
                            if (!isNoise) {
                                education.push(edu);
                            }
                        }
                    }
                    
                    return education;
                }
            """)
            
            # If we navigated away, go back to the original profile
            if show_all_navigated:
                try:
                    # Wait a bit before going back
                    await asyncio.sleep(1)
                    await page.goto(original_url, wait_until='domcontentloaded', timeout=15000)
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.debug(f"Could not navigate back to profile: {e}")
            
            # Filter out invalid entries and deduplicate
            seen = set()
            unique_education = []
            for edu in education:
                # Skip if school is generic or invalid
                school = edu.get('school', '').strip()
                if not school or school in ['Education', 'N/A', '']:
                    continue
                
                # Additional filtering for noise
                school_lower = school.lower()
                degree = edu.get('degree', '').strip()
                dates = edu.get('dates', '').strip()
                degree_lower = degree.lower()
                dates_lower = dates.lower()
                
                # FILTER OUT SALES NAVIGATOR INSIGHTS
                description = edu.get('description', '').strip()
                description_lower = description.lower()
                is_sales_navigator = (
                    'follows your company' in school_lower or
                    'viewed your profile' in school_lower or
                    'aware of your brand' in school_lower or
                    'free insight' in school_lower or
                    'sales navigator' in school_lower or
                    'follows your company' in degree_lower or
                    'viewed your profile' in degree_lower or
                    'aware of your brand' in degree_lower or
                    'free insight' in degree_lower or
                    'sales navigator' in degree_lower or
                    'free insight' in description_lower or
                    'sales navigator' in description_lower
                )
                
                # FILTER OUT PERSON NAMES - check if school looks like a person name
                name_pattern = re.match(r'^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}$', school)
                looks_like_name = bool(name_pattern) and len(school) < 60
                
                # Filter out noise patterns
                is_noise = (
                    # Sales Navigator insights
                    is_sales_navigator or
                    # Follower/member counts
                    bool(re.search(r'\d+[,\s]*\d+[,\s]*\d+\s+followers', school, re.IGNORECASE)) or
                    bool(re.search(r'\d+[,\s]*\d+[,\s]*\d+\s+followers', degree, re.IGNORECASE)) or
                    bool(re.search(r'\d+[,\s]*\d+[,\s]*\d+\s+followers', dates, re.IGNORECASE)) or
                    bool(re.search(r'\d+[,\s]*\d+[,\s]*\d+\s+members', school, re.IGNORECASE)) or
                    bool(re.search(r'\d+[,\s]*\d+[,\s]*\d+\s+members', degree, re.IGNORECASE)) or
                    bool(re.search(r'\d+[,\s]*\d+[,\s]*\d+\s+members', dates, re.IGNORECASE)) or
                    # Common companies (not schools)
                    school_lower in ['fiverr', 'upwork', 'freelancer', 'peopleperhour'] or
                    'fiverr' in school_lower or
                    'upwork' in school_lower or
                    'freelancer' in school_lower or
                    # Generic skill/course names
                    school_lower in ['logo design', 'creative thinking'] or
                    (school_lower.endswith('design') and len(school) < 20) or
                    school_lower.endswith('thinking') or
                    # Newsletter/company names
                    'toolbox' in school_lower or
                    'newsletter' in school_lower or
                    ('creator' in school_lower and 'toolbox' in school_lower) or
                    # Ranking text
                    degree_lower in ['· 3rd', '· 2nd', '· 1st'] or
                    degree_lower.startswith('· ') or
                    # Published/company info
                    'published' in dates_lower or
                    'weekly' in dates_lower or
                    'followers' in dates_lower or
                    'members' in dates_lower or
                    # Company URLs
                    'linkedin.com/company' in school or
                    # Person names (unless they have degree/school indicators)
                    (looks_like_name and
                     'university' not in school_lower and
                     'college' not in school_lower and
                     'school' not in school_lower and
                     'institute' not in school_lower and
                     'bachelor' not in degree_lower and
                     'master' not in degree_lower and
                     'degree' not in degree_lower and
                     'bs' not in degree_lower and
                     'bsc' not in degree_lower and
                     'ms' not in degree_lower and
                     'msc' not in degree_lower and
                     'Grade:' not in degree and
                     not re.search(r'\d{4}', dates)) or
                    # Very short names (likely not schools unless abbreviations)
                    (len(school) < 5 and not bool(re.match(r'^[A-Z]{2,5}$', school)))
                )
                
                if is_noise:
                    logger.debug(f"[EDUCATION] Filtering out noise entry: {school} | {degree} | {dates}")
                    continue
                
                # Create deduplication key
                key = f"{school}|{edu.get('degree', 'N/A')}|{edu.get('start_date', 'N/A')}"
                if key not in seen:
                    seen.add(key)
                    unique_education.append(edu)
            
            logger.info(f"[EDUCATION] Extracted {len(unique_education)} unique education entries")
            return unique_education
            
        except Exception as e:
            logger.warning(f"Error extracting education: {e}")
            # Try to navigate back if we were on education page
            if 'show_all_navigated' in locals() and show_all_navigated and original_url:
                try:
                    await page.goto(original_url, wait_until='domcontentloaded', timeout=10000)
                except:
                    pass
            return education
    
    async def _extract_skills(self, page: Page, all_text: str) -> List[str]:
        """Extract skills list - handles 'Show all skills' button and extracts from detail page"""
        skills = []
        original_url = page.url
        
        try:
            # Store original URL for navigation back
            logger.debug(f"[SKILLS] Extracting skills from: {original_url}")
            
            # Check for "Show all skills" button - improved detection
            # Handles patterns like "Show all skills", "Show all 16 skills", "Show all 24 skills", etc.
            show_all_btn = None
            show_all_selectors = [
                'a[id*="navigation-index-Show-all"]',
                'a[id*="Show-all"]',
                'a[id*="navigation-index-see-all-skills"]',
                'a[href*="/details/skills"]',
                'a.pvs-navigation__text',
                'a[class*="pvs-navigation"]',
            ]
            
            # Try to find "Show all skills" button using selectors
            for selector in show_all_selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    for elem in elements:
                        try:
                            text = await elem.inner_text()
                            href = await elem.get_attribute('href')
                            
                            # Check if it's a "Show all X skills" button (handles "Show all 16 skills", "Show all skills", etc.)
                            if href and '/details/skills' in href:
                                text_lower = text.lower().strip()
                                # Match: "Show all skills", "Show all 16 skills", "Show all 24 skills", etc.
                                if ('show all' in text_lower or 'show' in text_lower) and 'skill' in text_lower:
                                    show_all_btn = elem
                                    logger.debug(f"[SKILLS] Found 'Show all skills' button via selector: {text}")
                                    break
                        except:
                            continue
                    if show_all_btn:
                        break
                except:
                    continue
            
            # Also try finding by text content with more flexible matching (comprehensive search)
            if not show_all_btn:
                try:
                    all_links = await page.query_selector_all('a')
                    for link in all_links:
                        try:
                            link_text = await link.inner_text()
                            href = await link.get_attribute('href')
                            if href and '/details/skills' in href:
                                link_text_lower = link_text.lower().strip()
                                # Match patterns like "Show all 24 skills", "Show all skills", "Show all 16 skills", etc.
                                # Also handle variations like "See all skills"
                                if (('show all' in link_text_lower or 'show' in link_text_lower or 'see all' in link_text_lower) 
                                    and 'skill' in link_text_lower):
                                    show_all_btn = link
                                    logger.debug(f"[SKILLS] Found 'Show all skills' button by text search: {link_text}")
                                    break
                        except:
                            continue
                except:
                    pass
            
            # Final fallback: Use JavaScript to find the button (most comprehensive)
            if not show_all_btn:
                try:
                    show_all_btn_info = await page.evaluate("""
                        () => {
                            const links = document.querySelectorAll('a[href*="/details/skills"]');
                            for (const link of links) {
                                const text = link.textContent.trim().toLowerCase();
                                // Match: "Show all X skills", "Show all skills", "See all skills"
                                if (text.includes('show all') || text.includes('see all')) {
                                    if (text.includes('skill')) {
                                        return {
                                            found: true,
                                            text: link.textContent.trim()
                                        };
                                    }
                                }
                            }
                            return { found: false };
                        }
                    """)
                    
                    if show_all_btn_info.get('found'):
                        # Find the element again using the href
                        elements = await page.query_selector_all('a[href*="/details/skills"]')
                        for elem in elements:
                            try:
                                text = await elem.inner_text()
                                if show_all_btn_info['text'].lower() in text.lower() or text.lower() in show_all_btn_info['text'].lower():
                                    show_all_btn = elem
                                    logger.debug(f"[SKILLS] Found 'Show all skills' button via JavaScript: {text}")
                                    break
                            except:
                                continue
                except Exception as e:
                    logger.debug(f"[SKILLS] JavaScript button search error: {e}")
                    pass
            
            # Navigate to skills detail page if "Show all" button found
            if show_all_btn:
                try:
                    href = await show_all_btn.get_attribute('href')
                    if href:
                        if not href.startswith('http'):
                            href = f"https://www.linkedin.com{href}"
                        
                        logger.info(f"[SKILLS] Navigating to skills detail page: {href}")
                        await page.goto(href, wait_until='domcontentloaded', timeout=30000)
                        await asyncio.sleep(3)  # Wait for page to load
                        
                        # Scroll to load all skills (lazy loading) - improved scrolling
                        max_scrolls = 10
                        for scroll_attempt in range(max_scrolls):
                            # Scroll the page
                            await page.evaluate("""
                                () => {
                                    window.scrollTo(0, document.body.scrollHeight);
                                    const content = document.querySelector('.scaffold-finite-scroll__content, .pvs-list__container');
                                    if (content) {
                                        content.scrollTop = content.scrollHeight;
                                    }
                                }
                            """)
                            await asyncio.sleep(1)
                            
                            # Check if "Show more results" button exists and click it
                            try:
                                # Try multiple selectors for "Show more results" button
                                show_more_btn = None
                                show_more_selectors = [
                                    'button.scaffold-finite-scroll__load-button',
                                    'button[aria-label*="Show more"]',
                                    'button[aria-label*="Load more"]',
                                ]
                                
                                for selector in show_more_selectors:
                                    try:
                                        btn = await page.query_selector(selector)
                                        if btn:
                                            show_more_btn = btn
                                            break
                                    except:
                                        continue
                                
                                # Also try finding by text content
                                if not show_more_btn:
                                    try:
                                        all_buttons = await page.query_selector_all('button')
                                        for btn in all_buttons:
                                            try:
                                                btn_text = await btn.inner_text()
                                                if btn_text and ('show more' in btn_text.lower() or 'load more' in btn_text.lower()):
                                                    show_more_btn = btn
                                                    break
                                            except:
                                                continue
                                    except:
                                        pass
                                
                                if show_more_btn:
                                    is_visible = await show_more_btn.is_visible()
                                    if is_visible:
                                        await show_more_btn.scroll_into_view_if_needed()
                                        await show_more_btn.click()
                                        await asyncio.sleep(2)
                                        logger.debug(f"[SKILLS] Clicked 'Show more results' button (attempt {scroll_attempt + 1})")
                                    else:
                                        break
                                else:
                                    # No more button, we've loaded everything
                                    break
                            except:
                                # Button not found or error, continue scrolling
                                pass
                        
                        # Final scroll to ensure everything is loaded
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await asyncio.sleep(1)
                        
                        # Extract skills from the skills detail page using JavaScript
                        # On detail page, skills use data-field="skill_page_skill_topic" (not skill_card_skill_topic)
                        skills = await page.evaluate("""
                            () => {
                                const skills = [];
                                const seen = new Set();
                                
                                // Method 1: Look for skill_page_skill_topic (detail page) - primary method
                                const skillItems = document.querySelectorAll('li.pvs-list__paged-list-item, li.artdeco-list__item, li[class*="pvs-list"]');
                                
                                for (const item of skillItems) {
                                    let entityDiv = item.querySelector('[data-view-name="profile-component-entity"]');
                                    // Also try without the data-view-name attribute
                                    if (!entityDiv) {
                                        entityDiv = item;
                                    }
                                    
                                    // Try skill_page_skill_topic first (detail page)
                                    let skillLink = entityDiv.querySelector('a[data-field="skill_page_skill_topic"]');
                                    // Fallback to skill_card_skill_topic
                                    if (!skillLink) {
                                        skillLink = entityDiv.querySelector('a[data-field="skill_card_skill_topic"]');
                                    }
                                    // Also try any link with skill-related data-field
                                    if (!skillLink) {
                                        skillLink = entityDiv.querySelector('a[data-field*="skill"]');
                                    }
                                    
                                    if (skillLink) {
                                        // Try multiple selectors for skill name
                                        let skillNameEl = skillLink.querySelector('.t-bold span[aria-hidden="true"]');
                                        if (!skillNameEl) {
                                            skillNameEl = skillLink.querySelector('.hoverable-link-text.t-bold span[aria-hidden="true"]');
                                        }
                                        if (!skillNameEl) {
                                            skillNameEl = skillLink.querySelector('.display-flex.align-items-center.mr1.t-bold span[aria-hidden="true"]');
                                        }
                                        // Try without aria-hidden
                                        if (!skillNameEl) {
                                            skillNameEl = skillLink.querySelector('.t-bold span');
                                        }
                                        // Try direct text content
                                        if (!skillNameEl && skillLink.textContent.trim()) {
                                            const text = skillLink.textContent.trim();
                                            if (text && text.length > 0 && text.length < 100) {
                                                const skillName = text.split('\\n')[0].trim();
                                                if (skillName && 
                                                    skillName.length > 0 &&
                                                    skillName !== 'Skills' &&
                                                    skillName !== 'Learn more about these skills' &&
                                                    !skillName.toLowerCase().includes('discover') &&
                                                    !skillName.toLowerCase().includes('endorse') &&
                                                    !skillName.toLowerCase().includes('show') &&
                                                    !seen.has(skillName.toLowerCase())) {
                                                    skills.push(skillName);
                                                    seen.add(skillName.toLowerCase());
                                                }
                                            }
                                        }
                                        
                                        if (skillNameEl) {
                                            const skillName = skillNameEl.textContent.trim();
                                            const skillLower = skillName.toLowerCase();
                                            if (skillName && 
                                                skillName.length > 0 &&
                                                skillName.length < 100 &&
                                                skillName !== 'Skills' &&
                                                skillName !== 'Learn more about these skills' &&
                                                !skillLower.includes('discover') &&
                                                !skillLower.includes('endorse') &&
                                                !skillLower.includes('show') &&
                                                !skillLower.includes('see all') &&
                                                !seen.has(skillLower)) {
                                                skills.push(skillName);
                                                seen.add(skillLower);
                                            }
                                        }
                                    }
                                }
                                
                                // Method 2: Fallback - look for any skill name in the structure
                                if (skills.length === 0) {
                                    const allSkillLinks = document.querySelectorAll('a[data-field*="skill"]');
                                    for (const link of allSkillLinks) {
                                        // Try to get text from spans first
                                        const spans = link.querySelectorAll('span[aria-hidden="true"]');
                                        for (const span of spans) {
                                            const text = span.textContent.trim();
                                            const textLower = text.toLowerCase();
                                            if (text && 
                                                text.length > 0 &&
                                                text.length < 100 &&
                                                text !== 'Skills' &&
                                                !textLower.includes('discover') &&
                                                !textLower.includes('endorse') &&
                                                !textLower.includes('show') &&
                                                !textLower.includes('see all') &&
                                                !seen.has(textLower)) {
                                                skills.push(text);
                                                seen.add(textLower);
                                            }
                                        }
                                        // If no spans, try direct text
                                        if (spans.length === 0 && link.textContent.trim()) {
                                            const text = link.textContent.trim().split('\\n')[0].trim();
                                            const textLower = text.toLowerCase();
                                            if (text && 
                                                text.length > 0 &&
                                                text.length < 100 &&
                                                text !== 'Skills' &&
                                                !textLower.includes('discover') &&
                                                !textLower.includes('endorse') &&
                                                !textLower.includes('show') &&
                                                !textLower.includes('see all') &&
                                                !seen.has(textLower)) {
                                                skills.push(text);
                                                seen.add(textLower);
                                            }
                                        }
                                    }
                                }
                                
                                // Method 3: Last resort - look for any bold text in list items that might be skills
                                if (skills.length === 0) {
                                    const allListItems = document.querySelectorAll('li');
                                    for (const item of allListItems) {
                                        const boldText = item.querySelector('.t-bold');
                                        if (boldText) {
                                            const text = boldText.textContent.trim();
                                            const textLower = text.toLowerCase();
                                            // Filter out common non-skill text
                                            if (text && 
                                                text.length > 1 &&
                                                text.length < 50 &&
                                                !textLower.includes('skills') &&
                                                !textLower.includes('show') &&
                                                !textLower.includes('see') &&
                                                !textLower.includes('discover') &&
                                                !textLower.includes('endorse') &&
                                                !seen.has(textLower)) {
                                                skills.push(text);
                                                seen.add(textLower);
                                            }
                                        }
                                    }
                                }
                                
                                return skills;
                            }
                        """)
                        
                        logger.info(f"[SKILLS] Extracted {len(skills)} skills from detail page")
                        
                        # Navigate back to original profile URL
                        if original_url and original_url != page.url:
                            logger.debug(f"[SKILLS] Navigating back to: {original_url}")
                            await page.goto(original_url, wait_until='domcontentloaded', timeout=30000)
                            await asyncio.sleep(1)
                        
                        return skills if skills else []
                except Exception as e:
                    logger.warning(f"[SKILLS] Error navigating to skills page: {e}")
                    # Try to navigate back on error
                    try:
                        if original_url and original_url != page.url:
                            await page.goto(original_url, wait_until='domcontentloaded', timeout=30000)
                    except:
                        pass
            
            # Fallback: Extract skills from current page (no "Show all" button or navigation failed)
            logger.debug("[SKILLS] Extracting skills from current page (fallback)")
            
            # Use JavaScript to extract skills from the skills section only
            skills = await page.evaluate("""
                () => {
                    const skills = [];
                    // Find the skills section
                    let skillsSection = document.querySelector('#skills');
                    if (!skillsSection) {
                        // Try to find by heading
                        const headings = document.querySelectorAll('h2.pvs-header__title');
                        for (const heading of headings) {
                            const text = heading.textContent.trim().toLowerCase();
                            if (text === 'skills') {
                                skillsSection = heading.closest('section');
                                break;
                            }
                        }
                    }
                    
                    if (skillsSection) {
                        // Find all skill items within the skills section only
                        const skillItems = skillsSection.querySelectorAll('[data-view-name="profile-component-entity"]');
                        for (const item of skillItems) {
                            // Try skill_card_skill_topic (main page)
                            let skillLink = item.querySelector('a[data-field="skill_card_skill_topic"]');
                            // Fallback to skill_page_skill_topic
                            if (!skillLink) {
                                skillLink = item.querySelector('a[data-field="skill_page_skill_topic"]');
                            }
                            
                            if (skillLink) {
                                let skillNameEl = skillLink.querySelector('.t-bold span[aria-hidden="true"]');
                                if (!skillNameEl) {
                                    skillNameEl = skillLink.querySelector('.hoverable-link-text.t-bold span[aria-hidden="true"]');
                                }
                                if (!skillNameEl) {
                                    skillNameEl = skillLink.querySelector('.display-flex.align-items-center.mr1.t-bold span[aria-hidden="true"]');
                                }
                                
                                if (skillNameEl) {
                                    const skillName = skillNameEl.textContent.trim();
                                    if (skillName && 
                                        skillName.length > 0 &&
                                        skillName !== 'Skills' &&
                                        skillName !== 'Learn more about these skills' &&
                                        !skillName.toLowerCase().includes('discover') &&
                                        !skillName.toLowerCase().includes('endorse')) {
                                        skills.push(skillName);
                                    }
                                }
                            }
                        }
                    }
                    
                    return [...new Set(skills)];
                }
            """)
            
            logger.info(f"[SKILLS] Extracted {len(skills)} skills from current page")
            
        except Exception as e:
            logger.warning(f"[SKILLS] Error extracting skills: {e}")
        
        return skills if skills else []
    
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
        """Extract languages using JavaScript first, then text fallback"""
        languages = []
        try:
            # Method 1: JavaScript extraction targeting the Languages section
            js_languages = await page.evaluate("""
                () => {
                    const languages = [];
                    
                    // Find Languages section by ID or header
                    const langSection = document.querySelector('section[id*="languages"], div[id*="languages"]');
                    if (langSection) {
                        // Look for language entries in the section
                        const entries = langSection.querySelectorAll('[data-field="language_name"], .pvs-entity .t-bold span[aria-hidden="true"]');
                        for (const entry of entries) {
                            const text = entry.innerText.trim();
                            if (text && text.length > 1 && text.length < 50) {
                                languages.push(text);
                            }
                        }
                    }
                    
                    // Alternative: Look for section with Languages header
                    if (languages.length === 0) {
                        const headers = document.querySelectorAll('h2, div[class*="header"]');
                        for (const header of headers) {
                            if (header.innerText.trim().toLowerCase() === 'languages') {
                                const section = header.closest('section, .artdeco-card');
                                if (section) {
                                    const items = section.querySelectorAll('.pvs-entity .t-bold span[aria-hidden="true"]');
                                    for (const item of items) {
                                        const text = item.innerText.trim();
                                        if (text && text.length > 1 && text.length < 50) {
                                            languages.push(text);
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }
                    
                    return languages;
                }
            """)
            
            if js_languages and len(js_languages) > 0:
                # Validate extracted languages
                valid_languages = self._validate_languages(js_languages)
                if valid_languages:
                    return valid_languages
            
            # Method 2: Text-based fallback with strict validation
            lines = all_text.split('\n')
            lang_started = False
            
            # Known valid languages for validation
            known_languages = [
                'english', 'bengali', 'bangla', 'hindi', 'urdu', 'arabic', 'french', 'german',
                'spanish', 'portuguese', 'italian', 'russian', 'chinese', 'japanese', 'korean',
                'dutch', 'swedish', 'norwegian', 'danish', 'finnish', 'polish', 'turkish',
                'greek', 'hebrew', 'persian', 'thai', 'vietnamese', 'indonesian', 'malay',
                'tagalog', 'tamil', 'telugu', 'kannada', 'malayalam', 'marathi', 'gujarati',
                'punjabi', 'nepali', 'sinhala', 'burmese', 'khmer', 'lao', 'czech', 'slovak',
                'hungarian', 'romanian', 'bulgarian', 'croatian', 'serbian', 'slovenian',
                'ukrainian', 'swahili', 'afrikaans', 'zulu', 'xhosa', 'amharic'
            ]
            
            # Proficiency levels that follow language names
            proficiency_levels = [
                'native', 'bilingual', 'proficiency', 'fluent', 'professional', 'working',
                'elementary', 'limited', 'basic', 'intermediate', 'advanced', 'conversational'
            ]
            
            # Section end markers
            section_ends = ['Recommendations', 'Projects', 'Skills', 'Education', 'Experience',
                           'Organizations', 'Interests', 'More profiles', 'People you may know',
                           'Show all', 'Companies', 'Groups', 'Newsletters', 'Explore']
            
            for line in lines:
                line_stripped = line.strip()
                
                if line_stripped == 'Languages':
                    lang_started = True
                    continue
                
                if lang_started:
                    # Stop at next section or noise
                    if any(section in line_stripped for section in section_ends):
                        break
                    
                    # Skip obvious noise
                    if not line_stripped or len(line_stripped) <= 1 or len(line_stripped) > 60:
                        continue
                    
                    # Skip lines with URLs, buttons, numbers (follower counts)
                    if any(noise in line_stripped.lower() for noise in ['http', 'follow', 'button', 'connect', 'message', 'followers', 'connection', 'degree', '·', '@']):
                        break  # These indicate we've moved past languages section
                    
                    # Skip if contains digits (like "32,761,237 followers")
                    if re.search(r'\d{3,}', line_stripped):
                        break
                    
                    # Validate: is it a known language or a proficiency level?
                    line_lower = line_stripped.lower()
                    is_language = any(lang in line_lower for lang in known_languages)
                    is_proficiency = any(prof in line_lower for prof in proficiency_levels)
                    
                    if is_language or is_proficiency:
                        languages.append(line_stripped)
            
            languages = list(dict.fromkeys(languages))  # Remove duplicates
            
        except Exception as e:
            logger.warning(f"Error extracting languages: {e}")
        
        return languages
    
    def _validate_languages(self, languages: List[str]) -> List[str]:
        """Validate extracted languages - filter out non-language entries"""
        known_languages = [
            'english', 'bengali', 'bangla', 'hindi', 'urdu', 'arabic', 'french', 'german',
            'spanish', 'portuguese', 'italian', 'russian', 'chinese', 'japanese', 'korean',
            'dutch', 'swedish', 'norwegian', 'danish', 'finnish', 'polish', 'turkish',
            'greek', 'hebrew', 'persian', 'thai', 'vietnamese', 'indonesian', 'malay',
            'tagalog', 'tamil', 'telugu', 'kannada', 'malayalam', 'marathi', 'gujarati',
            'punjabi', 'nepali', 'sinhala', 'burmese', 'khmer', 'lao', 'czech', 'slovak',
            'hungarian', 'romanian', 'bulgarian', 'croatian', 'serbian', 'slovenian',
            'ukrainian', 'swahili', 'afrikaans', 'zulu', 'xhosa', 'amharic'
        ]
        
        proficiency_levels = [
            'native', 'bilingual', 'proficiency', 'fluent', 'professional', 'working',
            'elementary', 'limited', 'basic', 'intermediate', 'advanced', 'conversational'
        ]
        
        validated = []
        for lang in languages:
            if not lang or len(lang) > 60:
                continue
            lang_lower = lang.lower()
            # Check if it contains a known language or proficiency
            is_valid = any(known in lang_lower for known in known_languages) or \
                       any(prof in lang_lower for prof in proficiency_levels)
            if is_valid:
                validated.append(lang)
        
        return validated
    
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

    def _filter_personal_websites(self, urls: List[str]) -> List[str]:
        """
        Keep only person-specific websites. Filters out generic domains (gmail, bdjobs, linkedin, etc.),
        requires a meaningful path or a known personal-site platform, and removes root URLs when a more
        specific URL for the same domain exists.
        
        Key filtering rules:
        1. REJECT all generic domains (email providers, job sites, social media, etc.)
        2. REJECT all academic root domains (diu.edu.bd, etc.) - they're institutions, not personal
        3. REJECT root domains without meaningful paths (too likely to be generic)
        4. ACCEPT personal website platforms (sites.google.com, github.io, etc.) WITH a path
        5. ACCEPT URLs with meaningful person-specific paths
        """
        candidates: list[str] = []

        for url in urls:
            url_str = str(url or "").strip().rstrip('.,;:\'")')
            if not url_str:
                continue
            if url_str.startswith(("mailto:", "tel:", "javascript:")):
                continue

            # Skip obvious non-URLs
            if url_str.lower() in ["n/a", "none", "null", ""]:
                continue
            
            # Skip partial domains without http (these are noise from text extraction)
            # E.g., "gmail.com", "sites.google.com" without full URL
            if not url_str.startswith(("http://", "https://")):
                # Check if it looks like a bare domain (no path)
                if '/' not in url_str:
                    continue  # Skip bare domains like "gmail.com", "bdjobs.com"

            normalized = url_str if url_str.startswith(("http://", "https://")) else f"https://{url_str}"
            try:
                parsed = urlparse(normalized)
                netloc = (parsed.netloc or "").lower()
                path = (parsed.path or "").strip("/")
            except Exception:
                continue

            if not netloc:
                continue

            # ========== STEP 1: Check for personal platforms FIRST ==========
            # This takes priority over generic domain checks!
            is_personal_platform = any(platform in netloc for platform in self.PERSONAL_PLATFORMS)
            
            if is_personal_platform:
                # For personal platforms, require a meaningful path
                path_segments = [s for s in path.split('/') if s]
                if len(path_segments) >= 1:  # At least one path segment for personal platforms
                    candidates.append(normalized)
                    continue  # Skip all other checks - this is a valid personal website
            
            # ========== STEP 2: Filter out generic domains (STRICT) ==========
            is_generic = False
            
            # Check against generic domain list
            for generic_domain in self.GENERIC_DOMAINS:
                generic_lower = generic_domain.lower().strip('.')
                # Exact match
                if netloc == generic_lower:
                    is_generic = True
                    break
                # Subdomain match (e.g., mail.gmail.com)
                if netloc.endswith("." + generic_lower):
                    is_generic = True
                    break
                # Check if generic domain is contained in netloc
                if generic_lower in netloc and generic_lower != "com":  # Don't match just ".com"
                    is_generic = True
                    break
            
            if is_generic:
                continue
            
            # Check against generic domain suffix patterns
            for suffix in self.GENERIC_DOMAIN_SUFFIXES:
                suffix_lower = suffix.lower()
                # Check if the domain ends with this suffix (without a meaningful subdomain path)
                if netloc.endswith(suffix_lower):
                    # For academic domains, need substantial path
                    path_segments = [s for s in path.split('/') if s]
                    if len(path_segments) < 2:  # Need at least 2 path segments for academic domains
                        is_generic = True
                        break
            
            if is_generic:
                continue
            
            # ========== STEP 3: Check if it's a personal website ==========
            is_personal = False
            
            # For non-personal-platform domains that passed generic checks:
            # Require a meaningful path (multiple segments or personal indicators)
            path_segments = [s for s in path.split('/') if s]
            if len(path_segments) >= 2:
                # Check if path looks personal (contains profile-like words)
                path_lower = path.lower()
                personal_indicators = ['portfolio', 'profile', 'about', 'me', 'cv', 'resume', 
                                      'home', 'personal', 'blog', 'projects']
                if any(indicator in path_lower for indicator in personal_indicators):
                    is_personal = True
                else:  # Accept if it has substantial path (2+ segments)
                    is_personal = True
            elif len(path_segments) >= 1:
                # Single path segment - check for personal indicators
                path_lower = path.lower()
                personal_indicators = ['portfolio', 'profile', 'about', 'me', 'cv', 'resume', 
                                      'home', 'personal', 'blog', 'projects']
                if any(indicator in path_lower for indicator in personal_indicators):
                    is_personal = True
            
            # For root domains without path, REJECT - they're usually generic
            if not path or len(path.strip('/')) <= 1:
                if not is_personal_platform:
                    is_personal = False

            if not is_personal:
                continue

            candidates.append(normalized)

        # Deduplicate while preserving order
        deduped: list[str] = []
        for u in candidates:
            if u not in deduped:
                deduped.append(u)

        def _is_root(u: str) -> bool:
            try:
                parsed = urlparse(u)
            except Exception:
                return False
            return (parsed.path or "").strip("/") == ""

        final: list[str] = []
        for i, url_i in enumerate(deduped):
            try:
                parsed_i = urlparse(url_i)
            except Exception:
                continue

            if _is_root(url_i):
                # Skip root if same domain has a more specific URL
                has_specific = False
                for j, url_j in enumerate(deduped):
                    if i == j:
                        continue
                    try:
                        parsed_j = urlparse(url_j)
                    except Exception:
                        continue
                    if (parsed_j.netloc or "").lower() == (parsed_i.netloc or "").lower() and not _is_root(url_j):
                        has_specific = True
                        break
                if has_specific:
                    continue

            if url_i not in final:
                final.append(url_i)

        return final

    def _extract_emails_from_all_sections(self, profile_data: Dict, all_text: str) -> List[str]:
        """
        Extract emails from all profile sections (about, headline, experience, posts, projects, etc.)
        This is ADDITIONAL to contact info emails - does not replace them.
        """
        all_emails = set()
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        
        # 1. Extract from full page text (catches everything including posts)
        if all_text:
            page_emails = re.findall(email_pattern, all_text)
            all_emails.update(page_emails)
        
        # 2. Extract from About section
        about = profile_data.get('about', '')
        if about and isinstance(about, str):
            about_emails = re.findall(email_pattern, about)
            all_emails.update(about_emails)
        
        # 3. Extract from Headline
        headline = profile_data.get('headline', '')
        if headline and isinstance(headline, str):
            headline_emails = re.findall(email_pattern, headline)
            all_emails.update(headline_emails)
        
        # 4. Extract from Experience section
        experience = profile_data.get('experience', [])
        if experience and isinstance(experience, list):
            for exp in experience:
                if isinstance(exp, dict):
                    for value in exp.values():
                        if isinstance(value, str):
                            exp_emails = re.findall(email_pattern, value)
                            all_emails.update(exp_emails)
                elif isinstance(exp, str):
                    exp_emails = re.findall(email_pattern, exp)
                    all_emails.update(exp_emails)
        
        # 5. Extract from Projects section
        projects = profile_data.get('projects', [])
        if projects and isinstance(projects, list):
            for proj in projects:
                if isinstance(proj, dict):
                    for value in proj.values():
                        if isinstance(value, str):
                            proj_emails = re.findall(email_pattern, value)
                            all_emails.update(proj_emails)
                elif isinstance(proj, str):
                    proj_emails = re.findall(email_pattern, proj)
                    all_emails.update(proj_emails)
        
        # 6. Extract from Education section
        education = profile_data.get('education', [])
        if education and isinstance(education, list):
            for edu in education:
                if isinstance(edu, dict):
                    for value in edu.values():
                        if isinstance(value, str):
                            edu_emails = re.findall(email_pattern, value)
                            all_emails.update(edu_emails)
        
        # 7. Extract from Certifications
        certs = profile_data.get('certifications', [])
        if certs and isinstance(certs, list):
            for cert in certs:
                if isinstance(cert, dict):
                    for value in cert.values():
                        if isinstance(value, str):
                            cert_emails = re.findall(email_pattern, value)
                            all_emails.update(cert_emails)
        
        # 8. Extract from Recommendations
        recs = profile_data.get('recommendations', [])
        if recs and isinstance(recs, list):
            for rec in recs:
                if isinstance(rec, dict):
                    for value in rec.values():
                        if isinstance(value, str):
                            rec_emails = re.findall(email_pattern, value)
                            all_emails.update(rec_emails)
                elif isinstance(rec, str):
                    rec_emails = re.findall(email_pattern, rec)
                    all_emails.update(rec_emails)
        
        # Clean and validate emails
        valid_emails = []
        for email in all_emails:
            email = email.lower().strip()
            # Basic validation
            if '@' in email and '.' in email.split('@')[1]:
                # Skip obviously invalid emails
                if not email.endswith('.png') and not email.endswith('.jpg'):
                    if len(email) > 5 and len(email) < 100:
                        valid_emails.append(email)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_emails = []
        for email in valid_emails:
            if email not in seen:
                seen.add(email)
                unique_emails.append(email)
        
        logger.debug(f"[EMAIL] Extracted {len(unique_emails)} emails from profile sections")
        return unique_emails

    def _extract_phones_from_all_sections(self, profile_data: Dict, all_text: str) -> List[str]:
        """
        Extract phone numbers from all profile sections (about, headline, experience, posts, projects, etc.)
        This is ADDITIONAL to contact info phones - does not replace them.
        """
        all_phones = set()
        
        # Multiple phone patterns to catch different formats
        phone_patterns = [
            r'\+?\d{1,4}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}',  # International/general
            r'\b\d{11}\b',  # 11 digit (Bangladesh mobile)
            r'\b\d{10}\b',  # 10 digit (US/India)
            r'\+\d{1,3}\s?\d{4,14}',  # International with +
            r'01[3-9]\d{8}',  # Bangladesh mobile format
            r'\(\d{3}\)\s?\d{3}[-.]?\d{4}',  # (XXX) XXX-XXXX
            r'\d{3}[-.]?\d{3}[-.]?\d{4}',  # XXX-XXX-XXXX
        ]
        
        def extract_phones_from_text(text):
            phones = set()
            if not text:
                return phones
            for pattern in phone_patterns:
                matches = re.findall(pattern, text)
                for match in matches:
                    # Clean up the match
                    cleaned = re.sub(r'[^\d+]', '', str(match))
                    if cleaned:
                        phones.add(cleaned)
            return phones
        
        # 1. Extract from full page text
        if all_text:
            all_phones.update(extract_phones_from_text(all_text))
        
        # 2. Extract from About section
        about = profile_data.get('about', '')
        if about and isinstance(about, str):
            all_phones.update(extract_phones_from_text(about))
        
        # 3. Extract from Headline
        headline = profile_data.get('headline', '')
        if headline and isinstance(headline, str):
            all_phones.update(extract_phones_from_text(headline))
        
        # 4. Extract from Experience section
        experience = profile_data.get('experience', [])
        if experience and isinstance(experience, list):
            for exp in experience:
                if isinstance(exp, dict):
                    for value in exp.values():
                        if isinstance(value, str):
                            all_phones.update(extract_phones_from_text(value))
                elif isinstance(exp, str):
                    all_phones.update(extract_phones_from_text(exp))
        
        # 5. Extract from Projects section
        projects = profile_data.get('projects', [])
        if projects and isinstance(projects, list):
            for proj in projects:
                if isinstance(proj, dict):
                    for value in proj.values():
                        if isinstance(value, str):
                            all_phones.update(extract_phones_from_text(value))
                elif isinstance(proj, str):
                    all_phones.update(extract_phones_from_text(proj))
        
        # 6. Extract from Education section
        education = profile_data.get('education', [])
        if education and isinstance(education, list):
            for edu in education:
                if isinstance(edu, dict):
                    for value in edu.values():
                        if isinstance(value, str):
                            all_phones.update(extract_phones_from_text(value))
        
        # 7. Extract from Certifications
        certs = profile_data.get('certifications', [])
        if certs and isinstance(certs, list):
            for cert in certs:
                if isinstance(cert, dict):
                    for value in cert.values():
                        if isinstance(value, str):
                            all_phones.update(extract_phones_from_text(value))
        
        # 8. Extract from Recommendations
        recs = profile_data.get('recommendations', [])
        if recs and isinstance(recs, list):
            for rec in recs:
                if isinstance(rec, dict):
                    for value in rec.values():
                        if isinstance(value, str):
                            all_phones.update(extract_phones_from_text(value))
                elif isinstance(rec, str):
                    all_phones.update(extract_phones_from_text(rec))
        
        # Validate and filter phone numbers
        valid_phones = []
        for phone in all_phones:
            # Remove all non-digit except leading +
            cleaned = phone.lstrip('+')
            if cleaned.isdigit():
                # Valid phone should have 7-15 digits
                if 7 <= len(cleaned) <= 15:
                    # Skip numbers that look like years (1990-2030)
                    if not (len(cleaned) == 4 and 1900 <= int(cleaned) <= 2100):
                        # Restore + if it was there
                        final_phone = '+' + cleaned if phone.startswith('+') else cleaned
                        valid_phones.append(final_phone)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_phones = []
        for phone in valid_phones:
            normalized = re.sub(r'[^\d]', '', phone)  # Just digits for comparison
            if normalized not in seen:
                seen.add(normalized)
                unique_phones.append(phone)
        
        logger.debug(f"[PHONE] Extracted {len(unique_phones)} phones from profile sections")
        return unique_phones

    def _extract_github_from_all_sections(self, profile_data: Dict, all_text: str) -> List[str]:
        """
        Extract GitHub URLs from all profile sections (about, headline, experience, posts, projects, etc.)
        This is ADDITIONAL to contact info github_urls - does not replace them.
        """
        all_github = set()
        
        # GitHub URL patterns
        github_patterns = [
            r'https?://(?:www\.)?github\.com/[\w\-]+(?:/[\w\-\.]+)?',  # Full URL with optional repo
            r'github\.com/[\w\-]+(?:/[\w\-\.]+)?',  # Without protocol
        ]
        
        def extract_github_from_text(text):
            urls = set()
            if not text:
                return urls
            for pattern in github_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    # Ensure https:// prefix
                    if not match.startswith('http'):
                        match = 'https://' + match
                    urls.add(match)
            return urls
        
        # 1. Extract from full page text
        if all_text:
            all_github.update(extract_github_from_text(all_text))
        
        # 2. Extract from About section
        about = profile_data.get('about', '')
        if about and isinstance(about, str):
            all_github.update(extract_github_from_text(about))
        
        # 3. Extract from Headline
        headline = profile_data.get('headline', '')
        if headline and isinstance(headline, str):
            all_github.update(extract_github_from_text(headline))
        
        # 4. Extract from Experience section
        experience = profile_data.get('experience', [])
        if experience and isinstance(experience, list):
            for exp in experience:
                if isinstance(exp, dict):
                    for value in exp.values():
                        if isinstance(value, str):
                            all_github.update(extract_github_from_text(value))
                elif isinstance(exp, str):
                    all_github.update(extract_github_from_text(exp))
        
        # 5. Extract from Projects section
        projects = profile_data.get('projects', [])
        if projects and isinstance(projects, list):
            for proj in projects:
                if isinstance(proj, dict):
                    for value in proj.values():
                        if isinstance(value, str):
                            all_github.update(extract_github_from_text(value))
                elif isinstance(proj, str):
                    all_github.update(extract_github_from_text(proj))
        
        # 6. Extract from Education section
        education = profile_data.get('education', [])
        if education and isinstance(education, list):
            for edu in education:
                if isinstance(edu, dict):
                    for value in edu.values():
                        if isinstance(value, str):
                            all_github.update(extract_github_from_text(value))
        
        # 7. Extract from Certifications
        certs = profile_data.get('certifications', [])
        if certs and isinstance(certs, list):
            for cert in certs:
                if isinstance(cert, dict):
                    for value in cert.values():
                        if isinstance(value, str):
                            all_github.update(extract_github_from_text(value))
        
        # 8. Extract from Recommendations
        recs = profile_data.get('recommendations', [])
        if recs and isinstance(recs, list):
            for rec in recs:
                if isinstance(rec, dict):
                    for value in rec.values():
                        if isinstance(value, str):
                            all_github.update(extract_github_from_text(value))
                elif isinstance(rec, str):
                    all_github.update(extract_github_from_text(rec))
        
        # Clean and deduplicate
        valid_github = []
        seen = set()
        for url in all_github:
            # Normalize for comparison
            normalized = url.lower().rstrip('/')
            if normalized not in seen:
                seen.add(normalized)
                valid_github.append(url)
        
        logger.debug(f"[GITHUB] Extracted {len(valid_github)} GitHub URLs from profile sections")
        return valid_github

    def _extract_twitter_from_all_sections(self, profile_data: Dict, all_text: str) -> List[str]:
        """
        Extract Twitter handles/URLs from all profile sections (about, headline, experience, posts, projects, etc.)
        This is ADDITIONAL to contact info twitter - does not replace them.
        """
        all_twitter = set()
        
        # Twitter patterns - handles and URLs
        twitter_patterns = [
            r'https?://(?:www\.)?(?:twitter\.com|x\.com)/[\w]+',  # Full URL
            r'(?:twitter\.com|x\.com)/[\w]+',  # Without protocol
            r'@[\w]{1,15}(?=\s|$|[^\w])',  # @handle format
        ]
        
        def extract_twitter_from_text(text):
            handles = set()
            if not text:
                return handles
            for pattern in twitter_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    # Clean up and normalize
                    if match.startswith('@'):
                        handles.add(match)
                    elif 'twitter.com' in match.lower() or 'x.com' in match.lower():
                        # Extract handle from URL
                        parts = match.rstrip('/').split('/')
                        if parts:
                            handle = parts[-1]
                            if handle and not handle.startswith(('intent', 'share', 'home', 'search', 'explore', 'notifications', 'messages', 'settings', 'i', 'login', 'signup')):
                                handles.add(f"@{handle}")
            return handles
        
        # 1. Extract from full page text
        if all_text:
            all_twitter.update(extract_twitter_from_text(all_text))
        
        # 2. Extract from About section
        about = profile_data.get('about', '')
        if about and isinstance(about, str):
            all_twitter.update(extract_twitter_from_text(about))
        
        # 3. Extract from Headline
        headline = profile_data.get('headline', '')
        if headline and isinstance(headline, str):
            all_twitter.update(extract_twitter_from_text(headline))
        
        # 4. Extract from Experience section
        experience = profile_data.get('experience', [])
        if experience and isinstance(experience, list):
            for exp in experience:
                if isinstance(exp, dict):
                    for value in exp.values():
                        if isinstance(value, str):
                            all_twitter.update(extract_twitter_from_text(value))
                elif isinstance(exp, str):
                    all_twitter.update(extract_twitter_from_text(exp))
        
        # 5. Extract from Projects section
        projects = profile_data.get('projects', [])
        if projects and isinstance(projects, list):
            for proj in projects:
                if isinstance(proj, dict):
                    for value in proj.values():
                        if isinstance(value, str):
                            all_twitter.update(extract_twitter_from_text(value))
                elif isinstance(proj, str):
                    all_twitter.update(extract_twitter_from_text(proj))
        
        # 6. Extract from Education section
        education = profile_data.get('education', [])
        if education and isinstance(education, list):
            for edu in education:
                if isinstance(edu, dict):
                    for value in edu.values():
                        if isinstance(value, str):
                            all_twitter.update(extract_twitter_from_text(value))
        
        # 7. Extract from Certifications
        certs = profile_data.get('certifications', [])
        if certs and isinstance(certs, list):
            for cert in certs:
                if isinstance(cert, dict):
                    for value in cert.values():
                        if isinstance(value, str):
                            all_twitter.update(extract_twitter_from_text(value))
        
        # 8. Extract from Recommendations
        recs = profile_data.get('recommendations', [])
        if recs and isinstance(recs, list):
            for rec in recs:
                if isinstance(rec, dict):
                    for value in rec.values():
                        if isinstance(value, str):
                            all_twitter.update(extract_twitter_from_text(value))
                elif isinstance(rec, str):
                    all_twitter.update(extract_twitter_from_text(rec))
        
        # Clean and deduplicate
        valid_twitter = []
        seen = set()
        for handle in all_twitter:
            # Normalize for comparison (remove @ and lowercase)
            normalized = handle.lower().lstrip('@')
            if normalized not in seen and len(normalized) > 0:
                seen.add(normalized)
                # Store with @ prefix for consistency
                valid_twitter.append(f"@{normalized}" if not handle.startswith('@') else handle)
        
        logger.debug(f"[TWITTER] Extracted {len(valid_twitter)} Twitter handles from profile sections")
        return valid_twitter

    def _extract_instagram_from_all_sections(self, profile_data: Dict, all_text: str) -> List[str]:
        """
        Extract Instagram handles/URLs from all profile sections (about, headline, experience, posts, projects, etc.)
        This is ADDITIONAL to contact info instagram - does not replace them.
        """
        all_instagram = set()
        
        # Instagram patterns - handles and URLs
        instagram_patterns = [
            r'https?://(?:www\.)?instagram\.com/[\w\.]+',  # Full URL
            r'instagram\.com/[\w\.]+',  # Without protocol
            r'(?:instagram|ig)[:\s]*@?([\w\.]{1,30})',  # "instagram: handle" or "ig: handle"
        ]
        
        def extract_instagram_from_text(text):
            handles = set()
            if not text:
                return handles
            
            # Pattern 1 & 2: URLs
            url_pattern = r'(?:https?://)?(?:www\.)?instagram\.com/([\w\.]+)'
            url_matches = re.findall(url_pattern, text, re.IGNORECASE)
            for match in url_matches:
                if match and not match.startswith(('p/', 'reel/', 'stories/', 'explore/', 'accounts/', 'direct/', 'tv/')):
                    handles.add(f"@{match}")
            
            # Pattern 3: "instagram: handle" or "ig: @handle"
            labeled_pattern = r'(?:instagram|ig)[:\s]+@?([\w\.]{1,30})'
            labeled_matches = re.findall(labeled_pattern, text, re.IGNORECASE)
            for match in labeled_matches:
                if match and len(match) > 1:
                    handles.add(f"@{match}")
            
            return handles
        
        # 1. Extract from full page text
        if all_text:
            all_instagram.update(extract_instagram_from_text(all_text))
        
        # 2. Extract from About section
        about = profile_data.get('about', '')
        if about and isinstance(about, str):
            all_instagram.update(extract_instagram_from_text(about))
        
        # 3. Extract from Headline
        headline = profile_data.get('headline', '')
        if headline and isinstance(headline, str):
            all_instagram.update(extract_instagram_from_text(headline))
        
        # 4. Extract from Experience section
        experience = profile_data.get('experience', [])
        if experience and isinstance(experience, list):
            for exp in experience:
                if isinstance(exp, dict):
                    for value in exp.values():
                        if isinstance(value, str):
                            all_instagram.update(extract_instagram_from_text(value))
                elif isinstance(exp, str):
                    all_instagram.update(extract_instagram_from_text(exp))
        
        # 5. Extract from Projects section
        projects = profile_data.get('projects', [])
        if projects and isinstance(projects, list):
            for proj in projects:
                if isinstance(proj, dict):
                    for value in proj.values():
                        if isinstance(value, str):
                            all_instagram.update(extract_instagram_from_text(value))
                elif isinstance(proj, str):
                    all_instagram.update(extract_instagram_from_text(proj))
        
        # 6. Extract from Education section
        education = profile_data.get('education', [])
        if education and isinstance(education, list):
            for edu in education:
                if isinstance(edu, dict):
                    for value in edu.values():
                        if isinstance(value, str):
                            all_instagram.update(extract_instagram_from_text(value))
        
        # 7. Extract from Certifications
        certs = profile_data.get('certifications', [])
        if certs and isinstance(certs, list):
            for cert in certs:
                if isinstance(cert, dict):
                    for value in cert.values():
                        if isinstance(value, str):
                            all_instagram.update(extract_instagram_from_text(value))
        
        # 8. Extract from Recommendations
        recs = profile_data.get('recommendations', [])
        if recs and isinstance(recs, list):
            for rec in recs:
                if isinstance(rec, dict):
                    for value in rec.values():
                        if isinstance(value, str):
                            all_instagram.update(extract_instagram_from_text(value))
                elif isinstance(rec, str):
                    all_instagram.update(extract_instagram_from_text(rec))
        
        # Clean and deduplicate
        valid_instagram = []
        seen = set()
        for handle in all_instagram:
            # Normalize for comparison (remove @ and lowercase)
            normalized = handle.lower().lstrip('@')
            if normalized not in seen and len(normalized) > 0:
                seen.add(normalized)
                # Store with @ prefix for consistency
                valid_instagram.append(f"@{normalized}" if not handle.startswith('@') else handle)
        
        logger.debug(f"[INSTAGRAM] Extracted {len(valid_instagram)} Instagram handles from profile sections")
        return valid_instagram

    def _extract_facebook_from_all_sections(self, profile_data: Dict, all_text: str) -> List[str]:
        """
        Extract Facebook URLs from all profile sections (about, headline, experience, posts, projects, etc.)
        This is ADDITIONAL to contact info facebook - does not replace them.
        """
        all_facebook = set()
        
        # Facebook URL patterns
        facebook_patterns = [
            r'https?://(?:www\.)?facebook\.com/[\w\.]+',  # Full URL
            r'facebook\.com/[\w\.]+',  # Without protocol
            r'https?://(?:www\.)?fb\.com/[\w\.]+',  # Short fb.com URL
            r'fb\.com/[\w\.]+',  # fb.com without protocol
        ]
        
        def extract_facebook_from_text(text):
            urls = set()
            if not text:
                return urls
            
            for pattern in facebook_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    # Ensure https:// prefix
                    if not match.startswith('http'):
                        match = 'https://' + match
                    # Clean up and extract username/page
                    clean_url = match.rstrip('/')
                    # Skip system pages
                    skip_pages = ['login', 'signup', 'help', 'policies', 'settings', 'watch', 'marketplace', 'groups', 'events', 'gaming', 'fundraisers', 'pages', 'ads', 'business']
                    parts = clean_url.split('/')
                    if parts and parts[-1].lower() not in skip_pages:
                        urls.add(clean_url)
            return urls
        
        # 1. Extract from full page text
        if all_text:
            all_facebook.update(extract_facebook_from_text(all_text))
        
        # 2. Extract from About section
        about = profile_data.get('about', '')
        if about and isinstance(about, str):
            all_facebook.update(extract_facebook_from_text(about))
        
        # 3. Extract from Headline
        headline = profile_data.get('headline', '')
        if headline and isinstance(headline, str):
            all_facebook.update(extract_facebook_from_text(headline))
        
        # 4. Extract from Experience section
        experience = profile_data.get('experience', [])
        if experience and isinstance(experience, list):
            for exp in experience:
                if isinstance(exp, dict):
                    for value in exp.values():
                        if isinstance(value, str):
                            all_facebook.update(extract_facebook_from_text(value))
                elif isinstance(exp, str):
                    all_facebook.update(extract_facebook_from_text(exp))
        
        # 5. Extract from Projects section
        projects = profile_data.get('projects', [])
        if projects and isinstance(projects, list):
            for proj in projects:
                if isinstance(proj, dict):
                    for value in proj.values():
                        if isinstance(value, str):
                            all_facebook.update(extract_facebook_from_text(value))
                elif isinstance(proj, str):
                    all_facebook.update(extract_facebook_from_text(proj))
        
        # 6. Extract from Education section
        education = profile_data.get('education', [])
        if education and isinstance(education, list):
            for edu in education:
                if isinstance(edu, dict):
                    for value in edu.values():
                        if isinstance(value, str):
                            all_facebook.update(extract_facebook_from_text(value))
        
        # 7. Extract from Certifications
        certs = profile_data.get('certifications', [])
        if certs and isinstance(certs, list):
            for cert in certs:
                if isinstance(cert, dict):
                    for value in cert.values():
                        if isinstance(value, str):
                            all_facebook.update(extract_facebook_from_text(value))
        
        # 8. Extract from Recommendations
        recs = profile_data.get('recommendations', [])
        if recs and isinstance(recs, list):
            for rec in recs:
                if isinstance(rec, dict):
                    for value in rec.values():
                        if isinstance(value, str):
                            all_facebook.update(extract_facebook_from_text(value))
                elif isinstance(rec, str):
                    all_facebook.update(extract_facebook_from_text(rec))
        
        # Clean and deduplicate
        valid_facebook = []
        seen = set()
        for url in all_facebook:
            # Normalize for comparison
            normalized = url.lower().rstrip('/')
            if normalized not in seen:
                seen.add(normalized)
                valid_facebook.append(url)
        
        logger.debug(f"[FACEBOOK] Extracted {len(valid_facebook)} Facebook URLs from profile sections")
        return valid_facebook

    def _extract_whatsapp_from_all_sections(self, profile_data: Dict, all_text: str) -> List[str]:
        """
        Extract WhatsApp numbers/links from all profile sections (about, headline, experience, posts, projects, etc.)
        This is ADDITIONAL to contact info whatsapp - does not replace them.
        """
        all_whatsapp = set()
        
        # WhatsApp patterns - links and labeled numbers
        whatsapp_patterns = [
            r'https?://(?:wa\.me|api\.whatsapp\.com/send\?phone=|chat\.whatsapp\.com/)[\d\w]+',  # WhatsApp links
            r'wa\.me/(\d+)',  # wa.me short links
            r'(?:whatsapp|wa)[:\s]+\+?(\d[\d\s\-]{8,})',  # "whatsapp: +1234567890"
            r'(?:whatsapp|wa)[:\s]*@?(\d[\d\s\-]{8,})',  # "wa: 1234567890"
        ]
        
        def extract_whatsapp_from_text(text):
            numbers = set()
            if not text:
                return numbers
            
            # Pattern 1: wa.me links
            wa_link_pattern = r'(?:https?://)?wa\.me/(\d+)'
            wa_matches = re.findall(wa_link_pattern, text, re.IGNORECASE)
            for match in wa_matches:
                if match and len(match) >= 10:
                    numbers.add(f"+{match}")
            
            # Pattern 2: Full whatsapp URLs
            full_url_pattern = r'https?://(?:wa\.me|api\.whatsapp\.com/send\?phone=)(\d+)'
            url_matches = re.findall(full_url_pattern, text, re.IGNORECASE)
            for match in url_matches:
                if match and len(match) >= 10:
                    numbers.add(f"+{match}")
            
            # Pattern 3: Labeled whatsapp numbers "whatsapp: +8801712345678"
            labeled_pattern = r'(?:whatsapp|wa|whats\s*app)[:\s]+\+?(\d[\d\s\-]{8,})'
            labeled_matches = re.findall(labeled_pattern, text, re.IGNORECASE)
            for match in labeled_matches:
                cleaned = re.sub(r'[\s\-]', '', match)
                if cleaned and len(cleaned) >= 10:
                    numbers.add(f"+{cleaned}" if not cleaned.startswith('+') else cleaned)
            
            return numbers
        
        # 1. Extract from full page text
        if all_text:
            all_whatsapp.update(extract_whatsapp_from_text(all_text))
        
        # 2. Extract from About section
        about = profile_data.get('about', '')
        if about and isinstance(about, str):
            all_whatsapp.update(extract_whatsapp_from_text(about))
        
        # 3. Extract from Headline
        headline = profile_data.get('headline', '')
        if headline and isinstance(headline, str):
            all_whatsapp.update(extract_whatsapp_from_text(headline))
        
        # 4. Extract from Experience section
        experience = profile_data.get('experience', [])
        if experience and isinstance(experience, list):
            for exp in experience:
                if isinstance(exp, dict):
                    for value in exp.values():
                        if isinstance(value, str):
                            all_whatsapp.update(extract_whatsapp_from_text(value))
                elif isinstance(exp, str):
                    all_whatsapp.update(extract_whatsapp_from_text(exp))
        
        # 5. Extract from Projects section
        projects = profile_data.get('projects', [])
        if projects and isinstance(projects, list):
            for proj in projects:
                if isinstance(proj, dict):
                    for value in proj.values():
                        if isinstance(value, str):
                            all_whatsapp.update(extract_whatsapp_from_text(value))
                elif isinstance(proj, str):
                    all_whatsapp.update(extract_whatsapp_from_text(proj))
        
        # 6. Extract from Education section
        education = profile_data.get('education', [])
        if education and isinstance(education, list):
            for edu in education:
                if isinstance(edu, dict):
                    for value in edu.values():
                        if isinstance(value, str):
                            all_whatsapp.update(extract_whatsapp_from_text(value))
        
        # 7. Extract from Certifications
        certs = profile_data.get('certifications', [])
        if certs and isinstance(certs, list):
            for cert in certs:
                if isinstance(cert, dict):
                    for value in cert.values():
                        if isinstance(value, str):
                            all_whatsapp.update(extract_whatsapp_from_text(value))
        
        # 8. Extract from Recommendations
        recs = profile_data.get('recommendations', [])
        if recs and isinstance(recs, list):
            for rec in recs:
                if isinstance(rec, dict):
                    for value in rec.values():
                        if isinstance(value, str):
                            all_whatsapp.update(extract_whatsapp_from_text(value))
                elif isinstance(rec, str):
                    all_whatsapp.update(extract_whatsapp_from_text(rec))
        
        # Clean and deduplicate
        valid_whatsapp = []
        seen = set()
        for number in all_whatsapp:
            # Normalize for comparison (just digits)
            normalized = re.sub(r'[^\d]', '', number)
            if normalized not in seen and len(normalized) >= 10:
                seen.add(normalized)
                # Store with + prefix for consistency
                valid_whatsapp.append(f"+{normalized}" if not number.startswith('+') else number)
        
        logger.debug(f"[WHATSAPP] Extracted {len(valid_whatsapp)} WhatsApp numbers from profile sections")
        return valid_whatsapp

    def _extract_telegram_from_all_sections(self, profile_data: Dict, all_text: str) -> List[str]:
        """
        Extract Telegram handles/links from all profile sections (about, headline, experience, posts, projects, etc.)
        This is ADDITIONAL to contact info telegram - does not replace them.
        """
        all_telegram = set()
        
        def extract_telegram_from_text(text):
            handles = set()
            if not text:
                return handles
            
            # Pattern 1: t.me links - t.me/username or t.me/+invite
            tme_pattern = r'(?:https?://)?t\.me/([a-zA-Z0-9_+]+)'
            tme_matches = re.findall(tme_pattern, text, re.IGNORECASE)
            for match in tme_matches:
                if match and len(match) >= 3 and match not in ['joinchat', 'share', 'proxy', 'socks']:
                    handles.add(f"@{match}" if not match.startswith('+') else match)
            
            # Pattern 2: telegram.me links - telegram.me/username
            telegram_me_pattern = r'(?:https?://)?telegram\.me/([a-zA-Z0-9_]+)'
            telegram_me_matches = re.findall(telegram_me_pattern, text, re.IGNORECASE)
            for match in telegram_me_matches:
                if match and len(match) >= 3:
                    handles.add(f"@{match}")
            
            # Pattern 3: Labeled telegram handles "telegram: @handle" or "tg: handle"
            labeled_pattern = r'(?:telegram|tg|tlgrm)[:\s]+@?([a-zA-Z0-9_]{3,})'
            labeled_matches = re.findall(labeled_pattern, text, re.IGNORECASE)
            for match in labeled_matches:
                if match and len(match) >= 3:
                    handles.add(f"@{match.lstrip('@')}")
            
            # Pattern 4: Just @handle with context of telegram nearby
            # Look for telegram mentioned and @ handles nearby
            telegram_context = re.search(r'telegram|tg[\s:@]', text, re.IGNORECASE)
            if telegram_context:
                at_handles = re.findall(r'@([a-zA-Z0-9_]{5,32})', text)
                for handle in at_handles:
                    # Only add if it looks like a telegram handle (not email)
                    if not re.search(r'\.(com|org|net|io|co)', handle.lower()):
                        handles.add(f"@{handle}")
            
            return handles
        
        # 1. Extract from full page text
        if all_text:
            all_telegram.update(extract_telegram_from_text(all_text))
        
        # 2. Extract from About section
        about = profile_data.get('about', '')
        if about and isinstance(about, str):
            all_telegram.update(extract_telegram_from_text(about))
        
        # 3. Extract from Headline
        headline = profile_data.get('headline', '')
        if headline and isinstance(headline, str):
            all_telegram.update(extract_telegram_from_text(headline))
        
        # 4. Extract from Experience section
        experience = profile_data.get('experience', [])
        if experience and isinstance(experience, list):
            for exp in experience:
                if isinstance(exp, dict):
                    for value in exp.values():
                        if isinstance(value, str):
                            all_telegram.update(extract_telegram_from_text(value))
                elif isinstance(exp, str):
                    all_telegram.update(extract_telegram_from_text(exp))
        
        # 5. Extract from Projects section
        projects = profile_data.get('projects', [])
        if projects and isinstance(projects, list):
            for proj in projects:
                if isinstance(proj, dict):
                    for value in proj.values():
                        if isinstance(value, str):
                            all_telegram.update(extract_telegram_from_text(value))
                elif isinstance(proj, str):
                    all_telegram.update(extract_telegram_from_text(proj))
        
        # 6. Extract from Education section
        education = profile_data.get('education', [])
        if education and isinstance(education, list):
            for edu in education:
                if isinstance(edu, dict):
                    for value in edu.values():
                        if isinstance(value, str):
                            all_telegram.update(extract_telegram_from_text(value))
        
        # 7. Extract from Certifications
        certs = profile_data.get('certifications', [])
        if certs and isinstance(certs, list):
            for cert in certs:
                if isinstance(cert, dict):
                    for value in cert.values():
                        if isinstance(value, str):
                            all_telegram.update(extract_telegram_from_text(value))
        
        # 8. Extract from Recommendations
        recs = profile_data.get('recommendations', [])
        if recs and isinstance(recs, list):
            for rec in recs:
                if isinstance(rec, dict):
                    for value in rec.values():
                        if isinstance(value, str):
                            all_telegram.update(extract_telegram_from_text(value))
                elif isinstance(rec, str):
                    all_telegram.update(extract_telegram_from_text(rec))
        
        # Clean and deduplicate
        valid_telegram = []
        seen = set()
        for handle in all_telegram:
            # Normalize for comparison
            normalized = handle.lower().lstrip('@').rstrip('/')
            if normalized not in seen and len(normalized) >= 3:
                seen.add(normalized)
                # Store with @ prefix for consistency
                valid_telegram.append(f"@{normalized}" if not handle.startswith('@') else handle)
        
        logger.debug(f"[TELEGRAM] Extracted {len(valid_telegram)} Telegram handles from profile sections")
        return valid_telegram

    def _extract_skype_from_all_sections(self, profile_data: Dict, all_text: str) -> List[str]:
        """
        Extract Skype handles from all profile sections (about, headline, experience, posts, projects, etc.)
        This is ADDITIONAL to contact info skype - does not replace them.
        """
        all_skype = set()
        
        def extract_skype_from_text(text):
            handles = set()
            if not text:
                return handles
            
            # Pattern 1: Labeled skype handles "skype: handle" or "skype: live:handle"
            labeled_pattern = r'(?:skype|skp)[:\s]+([a-zA-Z0-9_\-\.\:]+)'
            labeled_matches = re.findall(labeled_pattern, text, re.IGNORECASE)
            for match in labeled_matches:
                if match and len(match) >= 3:
                    # Clean up the handle
                    cleaned = match.strip().rstrip('.,;:')
                    if cleaned and not cleaned.lower() in ['id', 'me', 'call', 'chat']:
                        handles.add(cleaned)
            
            # Pattern 2: live:username format (common Skype format)
            live_pattern = r'live:[a-zA-Z0-9_\-\.]+'
            live_matches = re.findall(live_pattern, text, re.IGNORECASE)
            for match in live_matches:
                if match and len(match) >= 6:
                    handles.add(match)
            
            # Pattern 3: Skype URL format join.skype.com/invite/xxx or skype:username
            skype_url_pattern = r'(?:join\.)?skype\.com/(?:invite/)?([a-zA-Z0-9_\-]+)'
            url_matches = re.findall(skype_url_pattern, text, re.IGNORECASE)
            for match in url_matches:
                if match and len(match) >= 3:
                    handles.add(match)
            
            # Pattern 4: skype:username?call or skype:username
            skype_uri_pattern = r'skype:([a-zA-Z0-9_\-\.]+)'
            uri_matches = re.findall(skype_uri_pattern, text, re.IGNORECASE)
            for match in uri_matches:
                if match and len(match) >= 3:
                    cleaned = match.split('?')[0]  # Remove ?call, ?chat etc
                    handles.add(cleaned)
            
            return handles
        
        # 1. Extract from full page text
        if all_text:
            all_skype.update(extract_skype_from_text(all_text))
        
        # 2. Extract from About section
        about = profile_data.get('about', '')
        if about and isinstance(about, str):
            all_skype.update(extract_skype_from_text(about))
        
        # 3. Extract from Headline
        headline = profile_data.get('headline', '')
        if headline and isinstance(headline, str):
            all_skype.update(extract_skype_from_text(headline))
        
        # 4. Extract from Experience section
        experience = profile_data.get('experience', [])
        if experience and isinstance(experience, list):
            for exp in experience:
                if isinstance(exp, dict):
                    for value in exp.values():
                        if isinstance(value, str):
                            all_skype.update(extract_skype_from_text(value))
                elif isinstance(exp, str):
                    all_skype.update(extract_skype_from_text(exp))
        
        # 5. Extract from Projects section
        projects = profile_data.get('projects', [])
        if projects and isinstance(projects, list):
            for proj in projects:
                if isinstance(proj, dict):
                    for value in proj.values():
                        if isinstance(value, str):
                            all_skype.update(extract_skype_from_text(value))
                elif isinstance(proj, str):
                    all_skype.update(extract_skype_from_text(proj))
        
        # 6. Extract from Education section
        education = profile_data.get('education', [])
        if education and isinstance(education, list):
            for edu in education:
                if isinstance(edu, dict):
                    for value in edu.values():
                        if isinstance(value, str):
                            all_skype.update(extract_skype_from_text(value))
        
        # 7. Extract from Certifications
        certs = profile_data.get('certifications', [])
        if certs and isinstance(certs, list):
            for cert in certs:
                if isinstance(cert, dict):
                    for value in cert.values():
                        if isinstance(value, str):
                            all_skype.update(extract_skype_from_text(value))
        
        # 8. Extract from Recommendations
        recs = profile_data.get('recommendations', [])
        if recs and isinstance(recs, list):
            for rec in recs:
                if isinstance(rec, dict):
                    for value in rec.values():
                        if isinstance(value, str):
                            all_skype.update(extract_skype_from_text(value))
                elif isinstance(rec, str):
                    all_skype.update(extract_skype_from_text(rec))
        
        # Clean and deduplicate
        valid_skype = []
        seen = set()
        for handle in all_skype:
            # Normalize for comparison
            normalized = handle.lower().strip()
            if normalized not in seen and len(normalized) >= 3:
                seen.add(normalized)
                valid_skype.append(handle)
        
        logger.debug(f"[SKYPE] Extracted {len(valid_skype)} Skype handles from profile sections")
        return valid_skype

    def _extract_youtube_from_all_sections(self, profile_data: Dict, all_text: str) -> List[str]:
        """
        Extract YouTube channel/video links from all profile sections (about, headline, experience, posts, projects, etc.)
        This is ADDITIONAL to contact info youtube - does not replace them.
        """
        all_youtube = set()
        
        def extract_youtube_from_text(text):
            urls = set()
            if not text:
                return urls
            
            # Pattern 1: Standard YouTube channel URLs
            channel_pattern = r'https?://(?:www\.)?youtube\.com/(?:channel/|c/|user/|@)([\w\-]+)'
            channel_matches = re.findall(channel_pattern, text, re.IGNORECASE)
            for match in channel_matches:
                if match and len(match) >= 2:
                    # Reconstruct full URL
                    urls.add(f"https://youtube.com/@{match}")
            
            # Pattern 2: Full YouTube URLs (capture the whole URL)
            full_url_pattern = r'https?://(?:www\.)?youtube\.com/(?:channel/|c/|user/|@)[\w\-]+'
            full_matches = re.findall(full_url_pattern, text, re.IGNORECASE)
            for match in full_matches:
                if match:
                    urls.add(match)
            
            # Pattern 3: youtu.be short links
            short_pattern = r'https?://youtu\.be/([\w\-]+)'
            short_matches = re.findall(short_pattern, text, re.IGNORECASE)
            for match in short_matches:
                if match and len(match) >= 5:
                    urls.add(f"https://youtu.be/{match}")
            
            # Pattern 4: YouTube video links
            video_pattern = r'https?://(?:www\.)?youtube\.com/watch\?v=([\w\-]+)'
            video_matches = re.findall(video_pattern, text, re.IGNORECASE)
            for match in video_matches:
                if match and len(match) >= 5:
                    urls.add(f"https://youtube.com/watch?v={match}")
            
            # Pattern 5: Labeled youtube "youtube: @channel" or "yt: channel"
            labeled_pattern = r'(?:youtube|yt)[:\s]+@?([a-zA-Z0-9_\-]+)'
            labeled_matches = re.findall(labeled_pattern, text, re.IGNORECASE)
            for match in labeled_matches:
                if match and len(match) >= 3 and match.lower() not in ['com', 'channel', 'video', 'watch']:
                    urls.add(f"https://youtube.com/@{match}")
            
            return urls
        
        # 1. Extract from full page text
        if all_text:
            all_youtube.update(extract_youtube_from_text(all_text))
        
        # 2. Extract from About section
        about = profile_data.get('about', '')
        if about and isinstance(about, str):
            all_youtube.update(extract_youtube_from_text(about))
        
        # 3. Extract from Headline
        headline = profile_data.get('headline', '')
        if headline and isinstance(headline, str):
            all_youtube.update(extract_youtube_from_text(headline))
        
        # 4. Extract from Experience section
        experience = profile_data.get('experience', [])
        if experience and isinstance(experience, list):
            for exp in experience:
                if isinstance(exp, dict):
                    for value in exp.values():
                        if isinstance(value, str):
                            all_youtube.update(extract_youtube_from_text(value))
                elif isinstance(exp, str):
                    all_youtube.update(extract_youtube_from_text(exp))
        
        # 5. Extract from Projects section
        projects = profile_data.get('projects', [])
        if projects and isinstance(projects, list):
            for proj in projects:
                if isinstance(proj, dict):
                    for value in proj.values():
                        if isinstance(value, str):
                            all_youtube.update(extract_youtube_from_text(value))
                elif isinstance(proj, str):
                    all_youtube.update(extract_youtube_from_text(proj))
        
        # 6. Extract from Education section
        education = profile_data.get('education', [])
        if education and isinstance(education, list):
            for edu in education:
                if isinstance(edu, dict):
                    for value in edu.values():
                        if isinstance(value, str):
                            all_youtube.update(extract_youtube_from_text(value))
        
        # 7. Extract from Certifications
        certs = profile_data.get('certifications', [])
        if certs and isinstance(certs, list):
            for cert in certs:
                if isinstance(cert, dict):
                    for value in cert.values():
                        if isinstance(value, str):
                            all_youtube.update(extract_youtube_from_text(value))
        
        # 8. Extract from Recommendations
        recs = profile_data.get('recommendations', [])
        if recs and isinstance(recs, list):
            for rec in recs:
                if isinstance(rec, dict):
                    for value in rec.values():
                        if isinstance(value, str):
                            all_youtube.update(extract_youtube_from_text(value))
                elif isinstance(rec, str):
                    all_youtube.update(extract_youtube_from_text(rec))
        
        # Clean and deduplicate
        valid_youtube = []
        seen = set()
        for url in all_youtube:
            # Normalize for comparison
            normalized = url.lower().rstrip('/').replace('www.', '')
            if normalized not in seen and len(url) >= 15:
                seen.add(normalized)
                valid_youtube.append(url)
        
        logger.debug(f"[YOUTUBE] Extracted {len(valid_youtube)} YouTube links from profile sections")
        return valid_youtube

    def _extract_twitter_url_from_all_sections(self, profile_data: Dict, all_text: str) -> List[str]:
        """
        Extract Twitter/X URLs and handles from all profile sections (about, headline, experience, posts, projects, etc.)
        This is ADDITIONAL to contact info twitter_url - does not replace them.
        """
        all_twitter = set()
        
        def extract_twitter_from_text(text):
            urls = set()
            if not text:
                return urls
            
            # Pattern 1: Twitter.com URLs
            twitter_url_pattern = r'https?://(?:www\.)?twitter\.com/([\w]+)'
            twitter_matches = re.findall(twitter_url_pattern, text, re.IGNORECASE)
            for match in twitter_matches:
                if match and len(match) >= 2 and match.lower() not in ['intent', 'share', 'home', 'search', 'login', 'signup', 'i']:
                    urls.add(f"https://twitter.com/{match}")
            
            # Pattern 2: X.com URLs (Twitter rebranded)
            x_url_pattern = r'https?://(?:www\.)?x\.com/([\w]+)'
            x_matches = re.findall(x_url_pattern, text, re.IGNORECASE)
            for match in x_matches:
                if match and len(match) >= 2 and match.lower() not in ['intent', 'share', 'home', 'search', 'login', 'signup', 'i']:
                    urls.add(f"https://x.com/{match}")
            
            # Pattern 3: Full Twitter/X URLs (capture whole URL)
            full_twitter_pattern = r'https?://(?:www\.)?(?:twitter|x)\.com/[\w]+'
            full_matches = re.findall(full_twitter_pattern, text, re.IGNORECASE)
            for match in full_matches:
                if match:
                    # Check it's not a system URL
                    username = match.split('/')[-1].lower()
                    if username not in ['intent', 'share', 'home', 'search', 'login', 'signup', 'i']:
                        urls.add(match)
            
            # Pattern 4: Labeled twitter handles "twitter: @handle" or "x: @handle"
            labeled_pattern = r'(?:twitter|x\.com|twtr)[:\s]+@?([a-zA-Z0-9_]+)'
            labeled_matches = re.findall(labeled_pattern, text, re.IGNORECASE)
            for match in labeled_matches:
                if match and len(match) >= 2 and match.lower() not in ['com', 'intent', 'share', 'home']:
                    urls.add(f"https://twitter.com/{match}")
            
            # Pattern 5: @handle with twitter/x context nearby
            twitter_context = re.search(r'twitter|\bx\.com\b|tweet', text, re.IGNORECASE)
            if twitter_context:
                at_handles = re.findall(r'@([a-zA-Z0-9_]{1,15})', text)
                for handle in at_handles:
                    if handle and len(handle) >= 2:
                        urls.add(f"https://twitter.com/{handle}")
            
            return urls
        
        # 1. Extract from full page text
        if all_text:
            all_twitter.update(extract_twitter_from_text(all_text))
        
        # 2. Extract from About section
        about = profile_data.get('about', '')
        if about and isinstance(about, str):
            all_twitter.update(extract_twitter_from_text(about))
        
        # 3. Extract from Headline
        headline = profile_data.get('headline', '')
        if headline and isinstance(headline, str):
            all_twitter.update(extract_twitter_from_text(headline))
        
        # 4. Extract from Experience section
        experience = profile_data.get('experience', [])
        if experience and isinstance(experience, list):
            for exp in experience:
                if isinstance(exp, dict):
                    for value in exp.values():
                        if isinstance(value, str):
                            all_twitter.update(extract_twitter_from_text(value))
                elif isinstance(exp, str):
                    all_twitter.update(extract_twitter_from_text(exp))
        
        # 5. Extract from Projects section
        projects = profile_data.get('projects', [])
        if projects and isinstance(projects, list):
            for proj in projects:
                if isinstance(proj, dict):
                    for value in proj.values():
                        if isinstance(value, str):
                            all_twitter.update(extract_twitter_from_text(value))
                elif isinstance(proj, str):
                    all_twitter.update(extract_twitter_from_text(proj))
        
        # 6. Extract from Education section
        education = profile_data.get('education', [])
        if education and isinstance(education, list):
            for edu in education:
                if isinstance(edu, dict):
                    for value in edu.values():
                        if isinstance(value, str):
                            all_twitter.update(extract_twitter_from_text(value))
        
        # 7. Extract from Certifications
        certs = profile_data.get('certifications', [])
        if certs and isinstance(certs, list):
            for cert in certs:
                if isinstance(cert, dict):
                    for value in cert.values():
                        if isinstance(value, str):
                            all_twitter.update(extract_twitter_from_text(value))
        
        # 8. Extract from Recommendations
        recs = profile_data.get('recommendations', [])
        if recs and isinstance(recs, list):
            for rec in recs:
                if isinstance(rec, dict):
                    for value in rec.values():
                        if isinstance(value, str):
                            all_twitter.update(extract_twitter_from_text(value))
                elif isinstance(rec, str):
                    all_twitter.update(extract_twitter_from_text(rec))
        
        # Clean and deduplicate
        valid_twitter = []
        seen = set()
        for url in all_twitter:
            # Normalize for comparison (treat twitter.com and x.com as same)
            normalized = url.lower().rstrip('/').replace('www.', '').replace('x.com', 'twitter.com')
            if normalized not in seen and len(url) >= 15:
                seen.add(normalized)
                valid_twitter.append(url)
        
        logger.debug(f"[TWITTER] Extracted {len(valid_twitter)} Twitter URLs from profile sections")
        return valid_twitter

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
            
            # Validate and clean phone numbers
            def is_valid_phone(phone: str) -> bool:
                """Validate phone number - reject timestamps, IDs, etc."""
                phone_clean = re.sub(r'[\s.\-()]+', '', phone.strip())
                
                # Must have at least some digits
                if not phone_clean:
                    return False
                
                # Check if it looks like a timestamp (YYYYMMDDHHMMSS pattern)
                if len(phone_clean) >= 14 and phone_clean.startswith(('202', '201', '200', '199')):
                    return False
                
                # Company IDs are often 8 digits without + prefix
                if len(phone_clean) == 8 and not phone.strip().startswith('+'):
                    return False
                
                # Valid phone: should be 10-15 digits with optional + prefix
                digits_only = re.sub(r'[^\d]', '', phone_clean)
                if len(digits_only) < 9 or len(digits_only) > 15:
                    return False
                
                # Should start with + or have a reasonable country/area code pattern
                if phone.strip().startswith('+'):
                    return True
                
                # US/CA format: 10 digits starting with area code
                if len(digits_only) == 10:
                    return True
                
                # Bangladesh format: starts with 01, 11 digits
                if len(digits_only) == 11 and digits_only.startswith('01'):
                    return True
                
                # International: 11-15 digits without +
                if len(digits_only) >= 11 and len(digits_only) <= 15:
                    return True
                
                return False
            
            validated_phones = [p.strip() for p in phones if is_valid_phone(p)]
            contact_info['phones'] = list(set(validated_phones)) if validated_phones else ['N/A']
            
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
            # Only extract explicit URLs with http(s) protocol - avoid extracting bare domains
            # Bare domains are too noisy and often generic (like gmail.com, diu.edu.bd, etc.)
            explicit_urls = re.findall(r'https?://[^\s<>"\)]+', contact_text)
            website_candidates: list[str] = [u.strip().rstrip('.,;:\'")') for u in explicit_urls if u.strip()]

            # DO NOT extract bare domains - they're too noisy and often generic
            # The href extraction in scrape_agent.py handles proper website extraction from DOM

            filtered_sites = self._filter_personal_websites(website_candidates)
            contact_info['websites'] = filtered_sites if filtered_sites else ['N/A']
            
            # ========== TWITTER EXTRACTION ==========
            twitter_patterns = [
                r'(?:twitter\.com/|x\.com/)(?P<handle>[\w\-]{1,15})',  # twitter.com/handle or x.com/handle
                r'https?://(?:www\.)?(?:twitter|x)\.com/([\w\-]+)',  # Full URL
            ]
            twitter_handles = []
            for pattern in twitter_patterns:
                matches = re.findall(pattern, contact_text, re.IGNORECASE)
                twitter_handles.extend(matches)
            
            # Validate Twitter handles - filter out invalid ones
            def is_valid_twitter_handle(handle: str) -> bool:
                """Validate Twitter handle"""
                handle = handle.strip().lstrip('@')
                if not handle:
                    return False
                # Must be 1-15 chars, alphanumeric + underscore
                if len(handle) > 15:
                    return False
                if not re.match(r'^[\w]+$', handle):
                    return False
                # Filter out common non-handle words
                invalid_handles = ['gmail', 'yahoo', 'hotmail', 'outlook', 'email', 
                                   'twitter', 'intent', 'share', 'home', 'search',
                                   'settings', 'messages', 'notifications', 'explore']
                if handle.lower() in invalid_handles:
                    return False
                return True
            
            validated_twitter = [h.lstrip('@') for h in twitter_handles if is_valid_twitter_handle(h)]
            contact_info['twitter'] = list(set(validated_twitter)) if validated_twitter else ['N/A']
            
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
            
            # ========== BIRTHDAY EXTRACTION (from Birthday section ONLY) ==========
            # Only extract from the Birthday section, NOT from Connected section
            birthday = None
            
            # Pattern to find Birthday section and extract date from it
            # Look for "Birthday" header followed by the date in the section
            birthday_section_pattern = r'Birthday\s*</h3>.*?<span[^>]*>\s*([A-Za-z]+\s+\d{1,2})\s*</span>'
            birthday_match = re.search(birthday_section_pattern, contact_text, re.IGNORECASE | re.DOTALL)
            
            if birthday_match:
                birthday = birthday_match.group(1).strip()
            else:
                # Fallback: Look for text-based pattern where Birthday label comes before the date
                # Split by sections and find Birthday section specifically
                lines = contact_text.split('\n')
                in_birthday_section = False
                for i, line in enumerate(lines):
                    line_clean = line.strip().lower()
                    if 'birthday' in line_clean and 'connected' not in line_clean:
                        in_birthday_section = True
                        continue
                    if in_birthday_section:
                        # Look for date pattern (Month Day like "July 12")
                        date_match = re.search(r'([A-Za-z]+\s+\d{1,2})(?!\s*,\s*\d{4})', line.strip())
                        if date_match:
                            potential_birthday = date_match.group(1).strip()
                            valid_months = ['january', 'february', 'march', 'april', 'may', 'june', 
                                          'july', 'august', 'september', 'october', 'november', 'december',
                                          'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
                            if any(month in potential_birthday.lower() for month in valid_months):
                                birthday = potential_birthday
                                break
                        # Stop if we hit another section header
                        if any(header in line_clean for header in ['connected', 'email', 'phone', 'address', 'website', 'twitter', 'skype']):
                            break
            
            contact_info['birthday'] = [birthday] if birthday else ['N/A']
            
            # ========== CONNECTED DATE EXTRACTION (from Connected section ONLY) ==========
            # Only extract from the Connected section, NOT from Birthday section
            connected_date = None
            
            # Since contact_text is plain text (HTML tags already stripped), we need to work with text patterns
            # Pattern 1: Try to find HTML pattern first (in case raw HTML is passed)
            connected_section_pattern = r'Connected\s*</h3>.*?<span[^>]*>\s*([A-Za-z]+\s+\d{1,2},?\s+\d{4})\s*</span>'
            connected_match = re.search(connected_section_pattern, contact_text, re.IGNORECASE | re.DOTALL)
            
            if connected_match:
                connected_date = connected_match.group(1).strip()
            else:
                # Pattern 2: Look for "Connected" followed by date pattern in plain text
                # Handle both "Connected\nNov 28, 2025" and "Connected Nov 28, 2025" patterns
                connected_text_pattern = r'Connected[^\n]*?\n\s*([A-Za-z]+\s+\d{1,2},?\s+\d{4})'
                connected_match = re.search(connected_text_pattern, contact_text, re.IGNORECASE | re.MULTILINE)
                
                if connected_match:
                    connected_date = connected_match.group(1).strip()
                else:
                    # Pattern 3: Fallback - Look for text-based pattern where Connected label comes before the date
                    lines = contact_text.split('\n')
                    in_connected_section = False
                    for i, line in enumerate(lines):
                        line_clean = line.strip().lower()
                        line_original = line.strip()
                        
                        # Check if this line contains "Connected" (case-insensitive)
                        if 'connected' in line_clean and len(line_clean.split()) <= 3:
                            # This is likely the "Connected" header line
                            in_connected_section = True
                            # Also check if date is on the same line
                            same_line_date = re.search(r'([A-Za-z]+\s+\d{1,2},?\s+\d{4})', line_original)
                            if same_line_date:
                                potential_connected = same_line_date.group(1).strip()
                                valid_months = ['january', 'february', 'march', 'april', 'may', 'june', 
                                              'july', 'august', 'september', 'october', 'november', 'december',
                                              'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
                                if any(month in potential_connected.lower() for month in valid_months):
                                    connected_date = potential_connected
                                    break
                            continue
                        
                        if in_connected_section:
                            # Look for date pattern with year (Month Day, Year like "Nov 28, 2025" or "Nov 27, 2025")
                            date_match = re.search(r'([A-Za-z]+\s+\d{1,2},?\s+\d{4})', line_original)
                            if date_match:
                                potential_connected = date_match.group(1).strip()
                                valid_months = ['january', 'february', 'march', 'april', 'may', 'june', 
                                              'july', 'august', 'september', 'october', 'november', 'december',
                                              'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
                                if any(month in potential_connected.lower() for month in valid_months):
                                    connected_date = potential_connected
                                    break
                            
                            # Stop if we hit another section header (but allow "Connected" to appear again)
                            if any(header in line_clean for header in ['birthday', 'email', 'phone', 'address', 'website', 'twitter', 'skype', 'github', 'linkedin']) and 'connected' not in line_clean:
                                break
                    
                    # Pattern 4: Last resort - search entire text for date patterns near "Connected"
                    if not connected_date:
                        # Find all occurrences of "Connected" and check nearby text
                        connected_indices = []
                        text_lower = contact_text.lower()
                        search_start = 0
                        while True:
                            idx = text_lower.find('connected', search_start)
                            if idx == -1:
                                break
                            connected_indices.append(idx)
                            search_start = idx + 1
                        
                        for idx in connected_indices:
                            # Extract text around "Connected" (up to 200 characters after)
                            context = contact_text[idx:idx+200]
                            date_match = re.search(r'([A-Za-z]+\s+\d{1,2},?\s+\d{4})', context)
                            if date_match:
                                potential_connected = date_match.group(1).strip()
                                valid_months = ['january', 'february', 'march', 'april', 'may', 'june', 
                                              'july', 'august', 'september', 'october', 'november', 'december',
                                              'jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
                                if any(month in potential_connected.lower() for month in valid_months):
                                    connected_date = potential_connected
                                    break
            
            contact_info['connected'] = [connected_date] if connected_date else ['N/A']
            
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

