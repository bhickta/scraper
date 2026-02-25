"""
Rapl Data Filler — fill Excel rows with GST data.

Thin wrapper around GstFillPipeline for the single-sheet Rapl file.
"""

import argparse
import logging
import os
import shutil
import sys
from datetime import datetime
from threading import Lock

import pandas as pd

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.config import DATA_DIR
from src.scripts.gst_fill_pipeline import (
    CheckpointManager,
    FillConfig,
    GstFillPipeline,
)
from src.services.gst_data_service import GstDataService
from src.services.rate_limiter import AdaptiveRateLimiter

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("src.core.base_scraper").setLevel(logging.CRITICAL)
logging.getLogger("src.services.gst_data_service").setLevel(logging.CRITICAL)

INPUT_FILE = os.path.join(DATA_DIR, "input/Estimated Data Rapl.xlsx")
OUTPUT_FILE = os.path.join(DATA_DIR, "input/Estimated Data Rapl_filled.xlsx")
CHECKPOINT_FILE = os.path.join(DATA_DIR, "input/.rapl_checkpoint_v2.json")


def main():
    parser = argparse.ArgumentParser(description="Optimized Rapl Data Filler")
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry GSTINs that failed in previous runs",
    )
    args = parser.parse_args()

    config = FillConfig(
        input_file=INPUT_FILE,
        output_file=OUTPUT_FILE,
        checkpoint_file=CHECKPOINT_FILE,
    )
    pipeline = GstFillPipeline(config)
    cache_lock = Lock()

    logger.info("Starting Rapl data filler")

    # Backup
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = INPUT_FILE.replace(".xlsx", f"_backup_{timestamp}.xlsx")
    try:
        shutil.copy2(INPUT_FILE, backup_file)
        logger.info(f"Backup created: {backup_file}")
    except Exception as e:
        logger.warning(f"Could not create backup: {e}")

    # Load data
    logger.info(f"Loading {INPUT_FILE}...")
    df = pd.read_excel(INPUT_FILE)
    logger.info(f"Loaded {len(df)} rows")

    # Load checkpoint
    checkpoint_mgr = CheckpointManager(CHECKPOINT_FILE, cache_lock)
    checkpoint = checkpoint_mgr.load()
    gstin_cache = checkpoint.get("gstin_cache", {})

    gst_service = GstDataService(cache=gstin_cache, cache_lock=cache_lock)
    rate_limiter = AdaptiveRateLimiter()

    # Extract unique GSTINs
    unique_gstins = (
        df["GSTIN"].dropna().astype(str).str.strip()
    )
    unique_gstins = list(unique_gstins[unique_gstins != ""].unique())
    logger.info(f"Found {len(unique_gstins)} unique GSTINs")

    # Filter what needs scraping
    gstins_to_scrape = []
    for gstin in unique_gstins:
        if gstin not in gstin_cache:
            gstins_to_scrape.append(gstin)
        elif args.retry_failed and gstin_cache[gstin] is None:
            gstins_to_scrape.append(gstin)
            del gstin_cache[gstin]

    logger.info(
        f"{len(gstin_cache)} cached, {len(gstins_to_scrape)} need scraping"
    )

    # Scrape
    if gstins_to_scrape and not pipeline.shutdown_requested:
        pipeline.scrape_gstins(gstins_to_scrape, gst_service, rate_limiter)

    stats = gst_service.get_cache_stats()
    logger.info(
        f"Cache: {stats['successful']} successful, "
        f"{stats['failed']} failed, {stats['total_cached']} total"
    )

    # Fill & save
    if not pipeline.shutdown_requested:
        df = pipeline.fill_dataframe(df, gst_service)

        # Deduplicate by GSTIN
        original_count = len(df)
        df_deduped = df.drop_duplicates(subset=["GSTIN"], keep="first")
        duplicates_removed = original_count - len(df_deduped)

        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate GSTINs")
            df = df_deduped

        logger.info(f"Saving {len(df)} rows to {OUTPUT_FILE}...")
        df.to_excel(OUTPUT_FILE, index=False)
        logger.info("Done!")

        checkpoint_mgr.cleanup()
    else:
        logger.info("Interrupted. Run again to resume.")


if __name__ == "__main__":
    main()
