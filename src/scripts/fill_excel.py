"""
Generic Excel GST Filler — fill ANY Excel file with GST data.

Takes an input Excel file, identifies the GSTIN column, and fills the data
using the shared GstFillPipeline.
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
    build_name_to_group_map,
    extract_local_cache,
)
from src.services.gst_data_service import GstDataService
from src.services.rate_limiter import AdaptiveRateLimiter

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Suppress noisy logs
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("src.core.base_scraper").setLevel(logging.CRITICAL)
logging.getLogger("src.services.gst_data_service").setLevel(logging.CRITICAL)

CHECKPOINT_FILE = os.path.join(DATA_DIR, "input/gst_cache.json")
LOCAL_CACHE_DB = os.path.join(DATA_DIR, "input/Estimated Data Rapl_filled.xlsx")


def main():
    parser = argparse.ArgumentParser(description="Generic GST Excel Filler")
    parser.add_argument("--input", required=True, help="Input Excel file path")
    parser.add_argument("--output", help="Output Excel file path (defaults to input_filled.xlsx)")
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry GSTINs that failed in previous runs (where cache is None)",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Ignore existing cache and force fresh scraping for all GSTINs in this file",
    )
    args = parser.parse_args()

    input_file = args.input
    output_file = args.output or input_file.replace(".xlsx", "_filled.xlsx")

    # Ensure output doesn't overwrite input if not specified
    if output_file == input_file:
        output_file = input_file.replace(".xlsx", "_filled.xlsx")

    # Define configuration
    config = FillConfig(
        input_file=input_file,
        output_file=output_file,
        checkpoint_file=CHECKPOINT_FILE,
        local_cache_files=[] if args.no_cache else [LOCAL_CACHE_DB, output_file]
    )
    pipeline = GstFillPipeline(config)
    cache_lock = Lock()

    logger.info(f"🚀 Starting GST filler for {input_file}")

    # Create backup
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = input_file.replace(".xlsx", f"_backup_{timestamp}.xlsx")
        shutil.copy2(input_file, backup_file)
        logger.info(f"📦 Backup created: {backup_file}")
    except Exception as e:
        logger.warning(f"⚠️ Could not create backup: {e}")

    # Load data
    logger.info(f"📂 Loading {input_file}...")
    df = pd.read_excel(input_file)
    logger.info(f"📊 Loaded {len(df)} rows")

    # Identify GSTIN column
    gst_col = next((c for c in df.columns if "gstin" in str(c).lower()), None)
    if not gst_col:
        logger.error("❌ No 'GSTIN' column found in the Excel file!")
        sys.exit(1)
    
    # Ensure it's named 'GSTIN' for the pipeline
    if gst_col != "GSTIN":
        logger.info(f"ℹ️ Map column '{gst_col}' to 'GSTIN'")
        df = df.rename(columns={gst_col: "GSTIN"})

    # 1. Extract local DB cache
    preloaded_cache = extract_local_cache(config.local_cache_files)

    # 2. Extract unique GSTINs
    unique_gstins = (
        df["GSTIN"].dropna().astype(str).str.strip()
    )
    unique_gstins = set(unique_gstins[unique_gstins != ""].unique())
    logger.info(f"🔍 Found {len(unique_gstins)} unique GSTINs in input")

    # 3. Load checkpoint
    checkpoint_mgr = CheckpointManager(CHECKPOINT_FILE, cache_lock)
    checkpoint = checkpoint_mgr.load()
    gstin_cache = checkpoint.get("gstin_cache", {})

    # If --no-cache, we don't want to use existing cache for scraping decisions
    if args.no_cache:
        logger.info("⚡ --no-cache active: Ignoring existing cache for scraping")
        # We still keep the service cache object, but we'll ensure we scrape
        # the GSTINs even if they are already in gstin_cache.
        # Simplest way: just pass an empty dict to the service for this file.
        actual_cache = {}
    else:
        actual_cache = gstin_cache

    gst_service = GstDataService(cache=actual_cache, cache_lock=cache_lock)
    rate_limiter = AdaptiveRateLimiter()

    # 4. Filter what needs scraping
    gstins_to_scrape = []
    for gstin in unique_gstins:
        if args.no_cache:
            gstins_to_scrape.append(gstin)
            continue
            
        if gstin in preloaded_cache:
            continue
        if gstin not in gstin_cache:
            gstins_to_scrape.append(gstin)
        elif args.retry_failed and gstin_cache[gstin] is None:
            gstins_to_scrape.append(gstin)
            del gstin_cache[gstin]

    logger.info(
        f"✅ Cache: {len(unique_gstins) - len(gstins_to_scrape)} known, 🛰️ {len(gstins_to_scrape)} need scraping"
    )

    # 5. Scrape missing data
    if gstins_to_scrape and not pipeline.shutdown_requested:
        pipeline.scrape_gstins(gstins_to_scrape, gst_service, rate_limiter)

    # 6. Fill the DataFrame and save
    if not pipeline.shutdown_requested:
        name_map = build_name_to_group_map(LOCAL_CACHE_DB)
        df_filled = pipeline.fill_dataframe(
            df, gst_service, preloaded_cache, name_map
        )

        logger.info(f"💾 Saving to {output_file}...")
        df_filled.to_excel(output_file, index=False)
        logger.info(f"⚡ Done! Saved to {output_file}")
    else:
        logger.info("⏸️ Interrupted. Progress saved.")


if __name__ == "__main__":
    main()
