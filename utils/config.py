"""
Configuration management
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Centralized configuration management"""
    
    def __init__(self, config_file: str = 'config/settings.yaml'):
        self.config_file = Path(config_file)
        self.settings = self._load_config()
        self._load_env_vars()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML"""
        default_config = {
            'scraping': {
                'headless': False,
                'max_profiles_per_search': 100,
                'delay_between_profiles': (15, 30),
                'max_retries': 3,
                'timeout': 60000,
                'use_stealth': True,
            },
            'browser': {
                'viewport_width': 1920,
                'viewport_height': 1080,
                'use_proxy': False,
                'proxy_server': '',
            },
            'database': {
                'path': 'data/linkedin_scraper.db',
                'auto_backup': True,
                'backup_interval': 100,
            },
            'export': {
                'auto_export': True,
                'formats': ['json', 'csv', 'excel'],
                'export_path': 'data/exports',
                'min_completeness': 50,
            },
            'logging': {
                'level': 'INFO',
                'file': 'logs/scraper.log',
                'max_size_mb': 10,
            },
            'anti_detection': {
                'random_delays': True,
                'human_behavior': True,
                'fingerprint_spoofing': True,
                'adaptive_rate_limiting': True,
            }
        }
        
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    user_config = yaml.safe_load(f) or {}
                return self._deep_merge(default_config, user_config)
            except:
                return default_config
        else:
            # Create default config file
            self.config_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_file, 'w') as f:
                yaml.dump(default_config, f, default_flow_style=False)
            return default_config
    
    def _load_env_vars(self):
        """Load environment variables"""
        self.LINKEDIN_EMAIL = os.getenv('LINKEDIN_EMAIL', '')
        self.LINKEDIN_PASSWORD = os.getenv('LINKEDIN_PASSWORD', '')
        self.HEADLESS = os.getenv('HEADLESS', 'False').lower() == 'true'
        self.USE_PROXY = os.getenv('USE_PROXY', 'False').lower() == 'true'
        self.PROXY_SERVER = os.getenv('PROXY_SERVER', '')
    
    def _deep_merge(self, base: Dict, update: Dict) -> Dict:
        """Deep merge dictionaries"""
        result = base.copy()
        for key, value in update.items():
            if isinstance(value, dict) and key in result and isinstance(result[key], dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    @property
    def scraping(self) -> Dict:
        return self.settings['scraping']
    
    @property
    def browser(self) -> Dict:
        return self.settings['browser']
    
    @property
    def database(self) -> Dict:
        return self.settings['database']
    
    @property
    def export(self) -> Dict:
        return self.settings['export']
    
    @property
    def logging_config(self) -> Dict:
        return self.settings['logging']
    
    @property
    def anti_detection(self) -> Dict:
        return self.settings['anti_detection']
    
    def get(self, key: str, default=None):
        """Get configuration value by dot notation"""
        keys = key.split('.')
        value = self.settings
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, {})
            else:
                return default
        return value if value != {} else default
