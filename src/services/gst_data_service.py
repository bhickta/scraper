"""
GST Data Service - Reusable GST extraction with caching and rate limiting.

This service provides a clean interface for extracting GST data with built-in:
- Caching (avoid duplicate requests)
- Rate limiting (prevent IP blocking)
- Thread-safe operations
- Persistent checkpoint support
"""

import logging
import time
import random
from threading import Lock
from typing import Dict, Optional, Any
from src.recipes.gst_recipe import GstExtractor

logger = logging.getLogger(__name__)


class GstDataService:
    """
    Service for fetching GST data with caching and rate limiting.
    
    Features:
    - Thread-safe caching to avoid duplicate GSTIN requests
    - Random delays between requests to prevent rate limiting
    - Persistent cache support via external checkpoint
    - Graceful error handling
    
    Usage:
        service = GstDataService(cache={}, cache_lock=Lock())
        data = service.get_gst_data("06AAFCC9473R1ZT")
    """
    
    def __init__(self, cache: Dict[str, Any], cache_lock: Lock, 
                 min_delay: float = 0.5, max_delay: float = 1.5):
        """
        Initialize the GST data service.
        
        Args:
            cache: Shared dictionary for caching GSTIN results
            cache_lock: Thread lock for cache synchronization
            min_delay: Minimum delay between requests (seconds)
            max_delay: Maximum delay between requests (seconds)
        """
        self.cache = cache
        self.cache_lock = cache_lock
        self.min_delay = min_delay
        self.max_delay = max_delay
        self._shutdown = False
    
    def shutdown(self):
        """Signal the service to stop processing new requests."""
        self._shutdown = True
    
    def get_gst_data(self, gstin: str) -> Optional[Dict[str, Any]]:
        """
        Fetch GST data for a given GSTIN.
        
        Implements deduplication via caching - if the same GSTIN is requested
        multiple times, only the first request hits the API.
        
        Args:
            gstin: The GSTIN to fetch data for
            
        Returns:
            Dictionary with GST data, or None if fetch failed
        """
        if self._shutdown:
            return None
        
        # Check cache first (deduplication)
        with self.cache_lock:
            if gstin in self.cache:
                logger.debug(f"Cache hit for GSTIN: {gstin}")
                return self.cache[gstin]
        
        # Not in cache - fetch from API with rate limiting
        try:
            # Random delay to avoid rate limiting (anti-IP-block strategy)
            delay = random.uniform(self.min_delay, self.max_delay)
            time.sleep(delay)
            
            url = f"https://gst.jamku.app/gstin/{gstin}"
            extractor = GstExtractor(base_url=url)
            results = extractor.extract()
            
            if results:
                data = results[0]
                # Cache the result
                with self.cache_lock:
                    self.cache[gstin] = data
                logger.debug(f"Fetched and cached data for GSTIN: {gstin}")
                return data
            else:
                logger.warning(f"No data found for GSTIN: {gstin}")
                # Cache the negative result to avoid retrying
                with self.cache_lock:
                    self.cache[gstin] = None
                return None
                
        except Exception as e:
            logger.error(f"Error fetching GST data for {gstin}: {e}")
            # Cache the error to avoid immediate retry
            with self.cache_lock:
                self.cache[gstin] = None
            return None
    
    def get_cache_stats(self) -> Dict[str, int]:
        """
        Get statistics about the cache.
        
        Returns:
            Dictionary with cache statistics
        """
        with self.cache_lock:
            total = len(self.cache)
            successful = sum(1 for v in self.cache.values() if v is not None)
            failed = total - successful
            
        return {
            'total_cached': total,
            'successful': successful,
            'failed': failed
        }
