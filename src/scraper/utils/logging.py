import logging
import sys
from scraper.config.settings import LOG_LEVEL, LOG_FORMAT


def get_logger(name, level=None):
    """
    Get a configured logger instance.

    Args:
        name (str): Logger name, typically __name__ of the calling module
        level (int, optional): Logging level. Defaults to LOG_LEVEL from settings.

    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)

    if level is None:
        level = LOG_LEVEL

    logger.setLevel(level)

    # Add console handler if no handlers exist
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(handler)

    return logger


def configure_file_logging(log_file_path, level=None):
    """
    Configure logging to output to both console and a file.

    Args:
        log_file_path (str): Path to the log file
        level (int, optional): Logging level. Defaults to LOG_LEVEL from settings.
    """
    root_logger = logging.getLogger()

    if level is None:
        level = LOG_LEVEL

    root_logger.setLevel(level)

    # Add file handler
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root_logger.addHandler(file_handler)

    # Ensure we don't duplicate messages
    root_logger.propagate = False
