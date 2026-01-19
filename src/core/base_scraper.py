import logging
import random
import time
import re
import requests
import json
import csv
import os
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from typing import Any, Dict, List, Optional
from src.core.interfaces import IDataExtractor

logger = logging.getLogger(__name__)


class BaseScraper(IDataExtractor):
    """
    Base scraper class that provides common functionality for all scrapers.
    This class implements the Template Method pattern for web scraping.
    """

    def __init__(self, **kwargs):
        self.base_url = kwargs.get('base_url', '').strip()
        self.content = kwargs.get('content')
        self.ua = UserAgent()
        self.session = requests.Session()
        self.soup = None

    @retry(stop=stop_after_attempt(5),
           wait=wait_exponential(multiplier=2, min=5, max=30),
           reraise=True)
    def fetch_page(self):
        """Fetch a web page with exponential backoff retry logic."""
        headers = {
            "User-Agent": self.ua.random,
            "Accept-Language": "en-US,en;q=0.9"
        }
        self.session.headers.update(headers)

        delay = random.uniform(2, 5)
        logger.info(
            f"Waiting {delay:.2f} seconds before request to {self.base_url}")
        time.sleep(delay)

        response = self.session.get(self.base_url, timeout=10)

        if response.status_code == 404:
            logger.warning(f"Page not found: {self.base_url} (404)")
            return ""

        if response.status_code == 429:
            logger.warning(f"Rate limit hit! Retrying... ({self.base_url})")
            raise Exception(f"Too many requests: {self.base_url} (429)")

        if response.status_code != 200:
            logger.error(
                f"Failed to fetch {self.base_url} (Status Code: {response.status_code})")
            raise Exception(
                f"Failed to fetch {self.base_url} (Status Code: {response.status_code})")

        return response.text

    def pre_parse(self, html_content):
        """Prepare the HTML content for parsing."""
        self.soup = BeautifulSoup(html_content, "html.parser")

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Main scraping method that orchestrates the scraping process.
        This implements the Template Method pattern.
        """
        try:
            html_content = self.content or kwargs.get('content') or self.fetch_page()
            self.pre_parse(html_content)
            return self.parse_page()
        except RetryError as e:
            logger.error(
                f"Retries failed for {self.base_url}. Error: {str(e)}")
            raise
        except Exception as e:
             logger.error(f"Extraction failed: {e}")
             raise

    def parse_page(self) -> List[Dict[str, Any]]:
        """
        Parse the HTML content. This method should be overridden by subclasses.
        """
        raise NotImplementedError(
            "Subclasses must implement parse_page method")

    def save(self, data: List[Dict[str, Any]], output_path: str):
        """
        Save the extracted data to a file.
        """
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        if output_path.endswith('.csv'):
             self._save_to_csv(data, output_path)
        elif output_path.endswith('.json'):
             self._save_to_json(data, output_path)
        else:
             raise ValueError("Unsupported format. Use .csv or .json")
    
    def _save_to_csv(self, data: List[Dict[str, Any]], output_path: str):
        if not data:
            logger.warning("No data to save")
            return
            
        keys = data[0].keys()
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)
            
    def _save_to_json(self, data: List[Dict[str, Any]], output_path: str):
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def get_html(self):
        """Return the prettified HTML content."""
        return self.soup.prettify() if self.soup else ""

    @staticmethod
    def normalize_whitespace(text):
        """Normalize whitespace in text."""
        if not text:
            return ""
        return re.sub(r'(\s)\1+', r'\1', text).strip()
