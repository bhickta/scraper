"""
BaseScraper — unified base class for all web scrapers.

Provides common HTTP fetching with retry logic, HTML parsing,
and the Template Method pattern for scraping.
"""

import logging
import random
import re
import time
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from src.config import (
    REQUEST_DELAY_MAX,
    REQUEST_DELAY_MIN,
    REQUEST_TIMEOUT,
    RETRY_ATTEMPTS,
    RETRY_MAX_WAIT,
    RETRY_MIN_WAIT,
    RETRY_MULTIPLIER,
)
from src.core.data_saver import DataSaverMixin
from src.core.interfaces import IDataExtractor

logger = logging.getLogger(__name__)


class BaseScraper(DataSaverMixin, IDataExtractor):
    """
    Base scraper class with retry logic, rate-limit awareness, and HTML parsing.

    Subclasses must implement ``parse_page()`` to extract data from ``self.soup``.
    """

    def __init__(self, **kwargs):
        self.base_url: str = kwargs.get("base_url", "").strip()
        self.content: Optional[str] = kwargs.get("content")
        self.ua = UserAgent()
        self.session = requests.Session()
        self.soup: Optional[BeautifulSoup] = None

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(RETRY_ATTEMPTS),
        wait=wait_exponential(
            multiplier=RETRY_MULTIPLIER, min=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT
        ),
        reraise=True,
    )
    def fetch_page(self) -> str:
        """Fetch a web page with exponential backoff retry logic."""
        headers = {
            "User-Agent": self.ua.random,
            "Accept-Language": "en-US,en;q=0.9",
        }
        self.session.headers.update(headers)

        delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
        logger.info(f"Waiting {delay:.2f}s before request to {self.base_url}")
        time.sleep(delay)

        response = self.session.get(self.base_url, timeout=REQUEST_TIMEOUT)

        if response.status_code == 404:
            logger.warning(f"Page not found: {self.base_url} (404)")
            return ""

        if response.status_code == 429:
            logger.warning(f"Rate limit hit! Retrying... ({self.base_url})")
            raise Exception(f"Too many requests: {self.base_url} (429)")

        if response.status_code != 200:
            logger.error(
                f"Failed to fetch {self.base_url} "
                f"(Status Code: {response.status_code})"
            )
            raise Exception(
                f"Failed to fetch {self.base_url} "
                f"(Status Code: {response.status_code})"
            )

        return response.text

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def pre_parse(self, html_content: str) -> None:
        """Prepare the HTML content for parsing."""
        self.soup = BeautifulSoup(html_content, "html.parser")

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Main scraping method — orchestrates fetch → parse pipeline.

        This implements the Template Method pattern.
        """
        try:
            html_content = (
                self.content or kwargs.get("content") or self.fetch_page()
            )
            self.pre_parse(html_content)
            return self.parse_page()
        except RetryError as e:
            logger.error(f"Retries failed for {self.base_url}. Error: {e}")
            raise
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise

    # Backward-compatible alias used by older recipes
    def scrape(self, content: Optional[str] = None) -> Any:
        """Alias for ``extract()`` — kept for backward compatibility."""
        return self.extract(content=content)

    def parse_page(self) -> List[Dict[str, Any]]:
        """Parse the HTML content. Subclasses must override this."""
        raise NotImplementedError("Subclasses must implement parse_page method")

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def get_html(self) -> str:
        """Return the prettified HTML content."""
        return self.soup.prettify() if self.soup else ""

    @staticmethod
    def normalize_whitespace(text: str) -> str:
        """Normalize whitespace in text."""
        if not text:
            return ""
        return re.sub(r"(\s)\1+", r"\1", text).strip()
