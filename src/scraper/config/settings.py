import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project paths
# scraper/config -> scraper -> src -> root
BASE_DIR = Path(__file__).parent.parent.parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = DATA_DIR / "output"

# Ensure directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Scraping settings
DEFAULT_TIMEOUT = 10
DEFAULT_RETRIES = 5
MIN_DELAY = 2
MAX_DELAY = 5

# Logging configuration
LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"

# User agent rotation
ROTATE_USER_AGENT = True

# Database settings
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{BASE_DIR}/scraper.db")

# API keys and credentials (from environment variables)
GST_API_KEY = os.getenv("GST_API_KEY", "")
PROXY_URL = os.getenv("PROXY_URL", "")
