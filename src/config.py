"""
Centralized configuration for the scraper project.

All magic numbers, file paths, and tunable parameters live here.
Values can be overridden via environment variables.
"""

import os
from dotenv import load_dotenv

load_dotenv()


def _int(key: str, default: int) -> int:
    return int(os.environ.get(key, default))


def _float(key: str, default: float) -> float:
    return float(os.environ.get(key, default))


def _str(key: str, default: str) -> str:
    return os.environ.get(key, default)


# --- Scraper retry settings ---
RETRY_ATTEMPTS = _int("RETRY_ATTEMPTS", 5)
RETRY_MIN_WAIT = _int("RETRY_MIN_WAIT", 5)
RETRY_MAX_WAIT = _int("RETRY_MAX_WAIT", 30)
RETRY_MULTIPLIER = _int("RETRY_MULTIPLIER", 2)

# --- Request delay (anti-rate-limit) ---
REQUEST_DELAY_MIN = _float("REQUEST_DELAY_MIN", 2.0)
REQUEST_DELAY_MAX = _float("REQUEST_DELAY_MAX", 5.0)

# --- GST fill pipeline ---
MAX_WORKERS = _int("MAX_WORKERS", 3)
BATCH_SIZE = _int("BATCH_SIZE", 10)
GST_BASE_URL = _str("GST_BASE_URL", "https://gst.jamku.app/gstin")

# --- Adaptive rate limiter ---
RATE_LIMIT_BASE_DELAY = _float("RATE_LIMIT_BASE_DELAY", 1.0)
RATE_LIMIT_MAX_DELAY = _float("RATE_LIMIT_MAX_DELAY", 10.0)
RATE_LIMIT_PAUSE_THRESHOLD = _int("RATE_LIMIT_PAUSE_THRESHOLD", 5)
RATE_LIMIT_PAUSE_DURATION = _int("RATE_LIMIT_PAUSE_DURATION", 30)
RATE_LIMIT_DECAY = _float("RATE_LIMIT_DECAY", 0.95)
RATE_LIMIT_BACKOFF_FACTOR = _float("RATE_LIMIT_BACKOFF_FACTOR", 2.0)
RATE_LIMIT_JITTER_MIN = _float("RATE_LIMIT_JITTER_MIN", 0.8)
RATE_LIMIT_JITTER_MAX = _float("RATE_LIMIT_JITTER_MAX", 1.2)

# --- Data directory ---
DATA_DIR = _str("SCRAPER_DATA_DIR", "data")

# --- HTTP settings ---
REQUEST_TIMEOUT = _int("REQUEST_TIMEOUT", 10)
