"""
Validation Agent: Validates scraped data quality
"""

import logging
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)


class ValidationAgent:
    """Agent for validating scraped profile data"""
    
    # Minimum required fields
    REQUIRED_FIELDS = ['profile_url', 'name']
    
    # Quality thresholds
    MIN_ABOUT_LENGTH = 20
    MIN_EXPERIENCE_ENTRIES = 0  # Allow empty
    MIN_SKILL_COUNT = 0
    
    def __init__(self):
        self.validation_errors = []
    
    def validate_profile(self, profile_data: Dict) -> tuple[bool, Dict]:
        """
        Validate profile data quality
        
        Returns:
            (is_valid, validation_report)
        """
        self.validation_errors = []
        report = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'score': 100,
            'data_completeness': self._calculate_completeness(profile_data)
        }
        
        # Check required fields
        for field in self.REQUIRED_FIELDS:
            if field not in profile_data or not profile_data[field]:
                self.validation_errors.append(f"Missing required field: {field}")
                report['errors'].append(f"Missing required field: {field}")
                report['is_valid'] = False
                report['score'] -= 20
        
        # Validate name
        if profile_data.get('name'):
            if not self._is_valid_name(profile_data['name']):
                self.validation_errors.append("Invalid name format")
                report['warnings'].append("Invalid name format")
                report['score'] -= 5
        
        # Validate URL
        if profile_data.get('profile_url'):
            if not profile_data['profile_url'].startswith('https://www.linkedin.com'):
                self.validation_errors.append("Invalid LinkedIn URL")
                report['errors'].append("Invalid LinkedIn URL")
                report['is_valid'] = False
                report['score'] -= 20
        
        # Check data quality
        if profile_data.get('about'):
            if len(profile_data['about']) < self.MIN_ABOUT_LENGTH:
                report['warnings'].append("Very short about section")
                report['score'] -= 10
        
        # Check experience
        experience = profile_data.get('experience', [])
        if experience and not isinstance(experience, list):
            report['errors'].append("Invalid experience format")
            report['score'] -= 10
        
        # Check skills
        skills = profile_data.get('skills', [])
        if skills and not isinstance(skills, list):
            report['errors'].append("Invalid skills format")
            report['score'] -= 10
        
        # Ensure score doesn't go below 0
        report['score'] = max(0, report['score'])
        
        logger.info(f"Validation report: {report}")
        
        return report['is_valid'], report
    
    def _is_valid_name(self, name: str) -> bool:
        """Validate name format"""
        if not isinstance(name, str):
            return False
        
        name = name.strip()
        
        # Name should have at least 2 characters
        if len(name) < 2:
            return False
        
        # Name should not be too long
        if len(name) > 200:
            return False
        
        # Should contain letters
        if not any(c.isalpha() for c in name):
            return False
        
        # Should not contain too many numbers
        if sum(1 for c in name if c.isdigit()) > len(name) * 0.3:
            return False
        
        return True
    
    def _calculate_completeness(self, profile_data: Dict) -> float:
        """Calculate data completeness percentage"""
        total_fields = 12  # Total possible fields
        filled_fields = 0
        
        important_fields = [
            'name', 'headline', 'about', 'location',
            'experience', 'education', 'skills',
            'certifications', 'projects'
        ]
        
        for field in important_fields:
            if field in profile_data and profile_data[field]:
                filled_fields += 1
        
        completeness = (filled_fields / len(important_fields)) * 100
        return round(completeness, 2)
    
    def batch_validate(self, profiles: List[Dict]) -> Dict:
        """Validate multiple profiles"""
        results = {
            'total': len(profiles),
            'valid': 0,
            'invalid': 0,
            'avg_completeness': 0,
            'avg_score': 0,
            'profiles': []
        }
        
        total_completeness = 0
        total_score = 0
        
        for profile in profiles:
            is_valid, report = self.validate_profile(profile)
            
            if is_valid:
                results['valid'] += 1
            else:
                results['invalid'] += 1
            
            total_completeness += report['data_completeness']
            total_score += report['score']
            
            results['profiles'].append({
                'name': profile.get('name'),
                'valid': is_valid,
                'completeness': report['data_completeness'],
                'score': report['score'],
                'errors': report['errors']
            })
        
        results['avg_completeness'] = round(total_completeness / len(profiles), 2) if profiles else 0
        results['avg_score'] = round(total_score / len(profiles), 2) if profiles else 0
        
        logger.info(f"Batch validation: {results['valid']}/{results['total']} valid")
        
        return results
