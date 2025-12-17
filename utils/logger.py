"""
Logging setup and configuration
"""

import logging
import logging.handlers
from pathlib import Path

def setup_logging(log_file: str = 'logs/scraper.log', level: str = 'INFO') -> logging.Logger:
    """Setup logging configuration"""
    
    # Create logs directory
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, level))
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(getattr(logging, level))
    
    # Console handler with UTF-8 encoding
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, level))
    console_handler.stream.reconfigure(encoding='utf-8', errors='replace')
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Get logger instance"""
    return logging.getLogger(name)
