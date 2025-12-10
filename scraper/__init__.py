"""
Advanced LinkedIn Bulk Profile Scraper
Multi-agent architecture with anti-detection capabilities
"""

__version__ = "2.0.0"
__author__ = "LinkedIn Scraper Team"

from .browser_controller import BrowserController
from .data_extractor import DataExtractor
from .human_behavior import HumanBehavior

__all__ = [
    "BrowserController",
    "DataExtractor",
    "HumanBehavior",
]
