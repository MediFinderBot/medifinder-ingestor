# utils/logger.py
import os
import logging
import sys
from pathlib import Path
from datetime import datetime

def setup_logger(level_name, log_file=None):
    """
    Set up logger with console and file handlers
    
    Args:
        level_name (str): Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file (str, optional): Log file name. Defaults to None.
        
    Returns:
        logging.Logger: Configured logger
    """
    # Convert level name to level
    level = getattr(logging, level_name)
    
    # Create logs directory if it doesn't exist
    logs_dir = Path(__file__).parent.parent / 'logs'
    logs_dir.mkdir(exist_ok=True)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(level)
    
    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if log_file is provided)
    if log_file:
        file_handler = logging.FileHandler(logs_dir / log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger