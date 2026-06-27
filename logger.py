import logging
import sys
from config import config

def get_logger(logger_name: str) -> logging.Logger:
    """
    Creates and returns a standardized logger for the application.
    """
    logger = logging.getLogger(logger_name)
    
    # Avoid duplicate logs if the logger is already configured
    if logger.hasHandlers():
        return logger

    # Set log level based on configuration
    log_level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(log_level)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    # Define a format (Time, Level, Module, Message)
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger