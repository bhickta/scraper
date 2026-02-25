"""
S_w Multi-Sheet Data Filler — fill multi-sheet Excel with GST data.

Reads multiple sheets from the S_w file, maps them to a unified format,
and uses the shared GstFillPipeline for scraping and filling.
"""

import argparse
import logging
import os
import sys
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

logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("src.core.base_scraper").setLevel(logging.CRITICAL)
logging.getLogger("src.services.gst_data_service").setLevel(logging.CRITICAL)

LOCAL_CACHE_DB = os.path.join(DATA_DIR, "input/Estimated Data Rapl_filled.xlsx")
INPUT_FILE = os.path.join(DATA_DIR, "input/Est Sale 25-26 For S_w.xlsx")
OUTPUT_FILE = os.path.join(DATA_DIR, "input/Est Sale 25-26 For S_w_filled.xlsx")
CHECKPOINT_FILE = os.path.join(DATA_DIR, "input/gst_cache.json")


def combine_sheets(input_file: str) -> pd.DataFrame:
    """Read multi-sheet Excel and combine into a single DataFrame."""
    logger.info(f"Loading workbook: {input_file}")
    xls = pd.ExcelFile(input_file)
    sheets_dict = pd.read_excel(xls, sheet_name=None)

    all_dfs = []
    for sheet_name, df in sheets_dict.items():
        logger.info(f"Mapping sheet: {sheet_name}")

        gst_col = next(
            (c for c in df.columns if "gstin" in str(c).lower()), None
        )
        name_col = next(
            (
                c
                for c in df.columns
                if "name" in str(c).lower() or "reciever" in str(c).lower()
            ),
            None,
        )
        sale_col = next(
            (
                c
                for c in df.columns
                if "sale" in str(c).lower() or "sum of" in str(c).lower()
            ),
            None,
        )

        mapped_df = pd.DataFrame()
        mapped_df["Supplier"] = [sheet_name] * len(df)
        mapped_df["Code"] = None
        mapped_df["Type"] = None
        mapped_df["GSTIN"] = df[gst_col] if gst_col else None
        mapped_df["Customer Name"] = df[name_col] if name_col else None
        mapped_df["Remark"] = None
        mapped_df["Customer Group"] = None
        mapped_df["Year"] = None
        mapped_df["Est Sale"] = df[sale_col] if sale_col else None
        mapped_df["Address"] = None

        all_dfs.append(mapped_df)

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Drop rows with no GSTIN and no Customer Name
    original_len = len(combined_df)
    combined_df = combined_df.dropna(subset=["GSTIN", "Customer Name"], how="all")
    logger.info(
        f"Combined {len(sheets_dict)} sheets into {len(combined_df)} rows "
        f"(dropped {original_len - len(combined_df)} empty rows)"
    )
    return combined_df


def main():
    parser = argparse.ArgumentParser(
        description="Multi-Sheet S_w Data Filler"
    )
    parser.add_argument(
        "--retry-failed", action="store_true", help="Retry failed GSTINs"
    )
    args = parser.parse_args()

    config = FillConfig(
        input_file=INPUT_FILE,
        output_file=OUTPUT_FILE,
        checkpoint_file=CHECKPOINT_FILE,
        local_cache_files=[LOCAL_CACHE_DB, OUTPUT_FILE],
    )
    pipeline = GstFillPipeline(config)
    cache_lock = Lock()

    logger.info("Starting S_w multi-sheet filler")

    # 1. Extract local DB cache
    preloaded_cache = extract_local_cache(config.local_cache_files)

    # 2. Combine sheets
    combined_df = combine_sheets(INPUT_FILE)

    # 3. Extract unique GSTINs
    df_g = combined_df["GSTIN"].dropna().astype(str).str.strip()
    df_g = df_g[df_g != ""]
    all_gstins = set(df_g.unique())
    logger.info(f"Found {len(all_gstins)} unique GSTINs")

    # 4. Load checkpoint
    checkpoint_mgr = CheckpointManager(CHECKPOINT_FILE, cache_lock)
    checkpoint = checkpoint_mgr.load()
    gstin_cache = checkpoint.get("gstin_cache", {})

    gst_service = GstDataService(cache=gstin_cache, cache_lock=cache_lock)
    rate_limiter = AdaptiveRateLimiter()

    # 5. Filter what needs scraping
    gstins_to_scrape = []
    for gstin in all_gstins:
        if gstin in preloaded_cache:
            continue
        if gstin not in gstin_cache:
            gstins_to_scrape.append(gstin)
        elif args.retry_failed and gstin_cache[gstin] is None:
            gstins_to_scrape.append(gstin)
            del gstin_cache[gstin]

    logger.info(
        f"{len(all_gstins) - len(gstins_to_scrape)} from caches, "
        f"{len(gstins_to_scrape)} need scraping"
    )

    # 6. Scrape
    if gstins_to_scrape and not pipeline.shutdown_requested:
        pipeline.scrape_gstins(gstins_to_scrape, gst_service, rate_limiter)

    stats = gst_service.get_cache_stats()
    logger.info(
        f"Cache: {stats['successful']} successful, {stats['failed']} failed"
    )

    # 7. Fill & Save
    if not pipeline.shutdown_requested:
        name_map = build_name_to_group_map(LOCAL_CACHE_DB)
        filled_df = pipeline.fill_dataframe(
            combined_df, gst_service, preloaded_cache, name_map
        )

        logger.info(f"Saving to {OUTPUT_FILE}...")
        filled_df.to_excel(OUTPUT_FILE, index=False, sheet_name="Sheet1")
        logger.info(f"Done! Saved to {OUTPUT_FILE}")
    else:
        logger.info("Interrupted.")


if __name__ == "__main__":
    main()
