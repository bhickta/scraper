"""
Scraper Package - A modular web scraping framework.

This package provides a structured approach to web scraping with plugin support,
allowing for extensible and maintainable scraping implementations.
"""

from scraper.core.base import BaseScraper
import logging
from scraper.config.settings import LOG_LEVEL, LOG_FORMAT

# Configure logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

# Version
__version__ = "1.0.0"

# Export public API
