"""
Adaptive Rate Limiter — automatically adjusts delays based on rate limit detection.

Tracks 429 errors and adjusts delays dynamically to avoid IP blocking.
"""

import logging
import random
from threading import Lock

from src.config import (
    RATE_LIMIT_BACKOFF_FACTOR,
    RATE_LIMIT_BASE_DELAY,
    RATE_LIMIT_DECAY,
    RATE_LIMIT_JITTER_MAX,
    RATE_LIMIT_JITTER_MIN,
    RATE_LIMIT_MAX_DELAY,
    RATE_LIMIT_PAUSE_DURATION,
    RATE_LIMIT_PAUSE_THRESHOLD,
)

logger = logging.getLogger(__name__)


class AdaptiveRateLimiter:
    """Tracks 429 errors and adjusts delays dynamically."""

    def __init__(
        self,
        base_delay: float = RATE_LIMIT_BASE_DELAY,
        max_delay: float = RATE_LIMIT_MAX_DELAY,
    ):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.current_delay = base_delay
        self.consecutive_429s = 0
        self.lock = Lock()

    def record_success(self) -> None:
        """Record a successful request — gradually reduce delay."""
        with self.lock:
            self.consecutive_429s = 0
            self.current_delay = max(
                self.base_delay, self.current_delay * RATE_LIMIT_DECAY
            )

    def record_429(self) -> None:
        """Record a 429 error — increase delay exponentially."""
        with self.lock:
            self.consecutive_429s += 1
            self.current_delay = min(
                self.max_delay, self.current_delay * RATE_LIMIT_BACKOFF_FACTOR
            )
            logger.warning(
                f"Rate limit detected! Slowing to {self.current_delay:.1f}s "
                f"(429 count: {self.consecutive_429s})"
            )

    def get_delay(self) -> float:
        """Get current delay with some randomness."""
        with self.lock:
            return self.current_delay * random.uniform(
                RATE_LIMIT_JITTER_MIN, RATE_LIMIT_JITTER_MAX
            )

    def should_pause(self) -> int:
        """Check if we should take a longer break. Returns pause duration in seconds."""
        with self.lock:
            if self.consecutive_429s >= RATE_LIMIT_PAUSE_THRESHOLD:
                logger.warning(
                    f"Too many rate limits ({self.consecutive_429s})! "
                    f"Taking {RATE_LIMIT_PAUSE_DURATION}s break..."
                )
                return RATE_LIMIT_PAUSE_DURATION
            return 0
