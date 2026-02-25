"""
GST Data Service — reusable GST extraction with caching and rate limiting.

Provides a clean interface for extracting GST data with built-in:
- Caching (avoid duplicate requests)
- Rate limiting (prevent IP blocking)
- Thread-safe operations
- Persistent checkpoint support
"""

import logging
import random
import time
from threading import Lock
from typing import Any, Dict, Optional

from src.config import GST_BASE_URL
from src.recipes.gst_recipe import GstExtractor

logger = logging.getLogger(__name__)


class GstDataService:
    """
    Service for fetching GST data with caching and rate limiting.

    Usage::

        service = GstDataService(cache={}, cache_lock=Lock())
        data = service.get_gst_data("06AAFCC9473R1ZT")
    """

    def __init__(
        self,
        cache: Dict[str, Any],
        cache_lock: Lock,
        min_delay: float = 0.5,
        max_delay: float = 1.5,
    ):
        self.cache = cache
        self.cache_lock = cache_lock
        self.min_delay = min_delay
        self.max_delay = max_delay
        self._shutdown = False

    def shutdown(self) -> None:
        """Signal the service to stop processing new requests."""
        self._shutdown = True

    def get_gst_data(self, gstin: str) -> Optional[Dict[str, Any]]:
        """
        Fetch GST data for a given GSTIN.

        Deduplicates via caching — repeated requests for the same GSTIN
        return cached results.

        Args:
            gstin: The GSTIN to fetch data for.

        Returns:
            Dictionary with GST data, or None if fetch failed.
        """
        if self._shutdown:
            return None

        # Check cache first
        with self.cache_lock:
            if gstin in self.cache:
                logger.debug(f"Cache hit for GSTIN: {gstin}")
                return self.cache[gstin]

        # Fetch from API with rate limiting
        try:
            delay = random.uniform(self.min_delay, self.max_delay)
            time.sleep(delay)

            url = f"{GST_BASE_URL}/{gstin}"
            extractor = GstExtractor(base_url=url)
            results = extractor.extract()

            if results:
                data = results[0]
                with self.cache_lock:
                    self.cache[gstin] = data
                logger.debug(f"Fetched and cached data for GSTIN: {gstin}")
                return data
            else:
                logger.warning(f"No data found for GSTIN: {gstin}")
                with self.cache_lock:
                    self.cache[gstin] = None
                return None

        except Exception as e:
            logger.error(f"Error fetching GST data for {gstin}: {e}")
            with self.cache_lock:
                self.cache[gstin] = None
            return None

    def get_cache_stats(self) -> Dict[str, int]:
        """Get statistics about the cache."""
        with self.cache_lock:
            total = len(self.cache)
            successful = sum(1 for v in self.cache.values() if v is not None)
            failed = total - successful

        return {
            "total_cached": total,
            "successful": successful,
            "failed": failed,
        }
