"""
GST Fill Pipeline — shared orchestration for filling Excel files with GST data.

Extracts the common logic from fill_rapl.py and fill_sw.py:
- Checkpoint management (save/load JSON caches)
- Parallel GSTIN scraping with adaptive rate limiting
- DataFrame filling from cached GST data
- Graceful shutdown on Ctrl+C
"""

import json
import logging
import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm import tqdm

from src.config import BATCH_SIZE, MAX_WORKERS
from src.services.gst_data_service import GstDataService
from src.services.rate_limiter import AdaptiveRateLimiter

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration data class
# ---------------------------------------------------------------------------


@dataclass
class FillConfig:
    """Configuration for a GST fill pipeline run."""

    input_file: str
    output_file: str
    checkpoint_file: str
    local_cache_files: List[str] = field(default_factory=list)
    max_workers: int = MAX_WORKERS
    batch_size: int = BATCH_SIZE


# ---------------------------------------------------------------------------
# Checkpoint Manager
# ---------------------------------------------------------------------------


class CheckpointManager:
    """Handles loading and saving of JSON checkpoint caches."""

    def __init__(self, checkpoint_file: str, cache_lock: Lock):
        self.checkpoint_file = checkpoint_file
        self.cache_lock = cache_lock

    def load(self) -> Dict[str, Any]:
        """Load checkpoint data if it exists."""
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, "r") as f:
                return json.load(f)
        return {"gstin_cache": {}}

    def save(self, gstin_cache: Dict[str, Any]) -> None:
        """Save checkpoint data atomically."""
        with self.cache_lock:
            with open(self.checkpoint_file, "w") as f:
                json.dump({"gstin_cache": gstin_cache}, f, indent=2)

    def cleanup(self) -> None:
        """Remove checkpoint file after successful completion."""
        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)


# ---------------------------------------------------------------------------
# Local Cache Extraction
# ---------------------------------------------------------------------------

# Column names that contain GST data
GST_DATA_COLUMNS = [
    "Legal Name", "Trade Name", "Status", "Registration Date",
    "City", "District", "State", "Pincode",
    "E-Invoice Mandatory", "Aggregate Turnover",
    "Central Jurisdiction", "State Jurisdiction", "HSN Codes",
]

# Mapping from Excel column names to GST data keys
EXCEL_TO_GST_KEY_MAP = {
    "Type": "Constitution",
    "Address": "Principal Place",
}


def extract_local_cache(filepaths: List[str]) -> Dict[str, Any]:
    """
    Extract local cache of GST data from Excel files.

    Reads previously filled Excel files and builds a GSTIN → data dict
    to avoid re-scraping.
    """
    cache: Dict[str, Any] = {}
    for filepath in filepaths:
        if not os.path.exists(filepath):
            logger.info(f"Local cache file missing: {filepath}")
            continue

        logger.info(f"Loading local DB: {filepath}")
        try:
            df = pd.read_excel(filepath)
            if "GSTIN" not in df.columns:
                logger.warning(f"'GSTIN' column not found in {filepath}")
                continue

            filled = 0
            for _, row in df.iterrows():
                gstin = (
                    str(row["GSTIN"]).strip()
                    if pd.notna(row["GSTIN"])
                    else None
                )
                if (
                    gstin
                    and gstin.lower() != "nan"
                    and pd.notna(row.get("Legal Name"))
                ):
                    data_dict = {col: row.get(col) for col in GST_DATA_COLUMNS}
                    data_dict["Constitution"] = row.get("Type")
                    data_dict["Principal Place"] = row.get("Address")
                    data_dict["Customer Group"] = row.get("Customer Group")
                    data_dict["Remark"] = row.get("Remark")
                    cache[gstin] = data_dict
                    filled += 1
            logger.info(f"Extracted {filled} GST records from {filepath}")
        except Exception as e:
            logger.error(f"Error loading local DB {filepath}: {e}")

    return cache


# ---------------------------------------------------------------------------
# Name → Customer Group mapping
# ---------------------------------------------------------------------------


def build_name_to_group_map(filepath: str) -> Dict[str, Dict]:
    """Build a map of customer/legal/trade name → Customer Group + Remark."""
    mapping: Dict[str, Dict] = {}
    if not os.path.exists(filepath):
        return mapping

    try:
        df = pd.read_excel(filepath)
        for _, row in df.iterrows():
            cg = row.get("Customer Group")
            rm = row.get("Remark")

            if pd.isna(cg) and pd.isna(rm):
                continue

            entry = {
                "Customer Group": cg if pd.notna(cg) else None,
                "Remark": rm if pd.notna(rm) else None,
            }

            for key in ["Customer Name", "Legal Name", "Trade Name"]:
                name = row.get(key)
                if pd.notna(name) and str(name).strip():
                    mapping[str(name).strip().upper()] = entry
    except Exception as e:
        logger.error(f"Error building name map from {filepath}: {e}")

    return mapping


# ---------------------------------------------------------------------------
# Scraping orchestration
# ---------------------------------------------------------------------------


class GstFillPipeline:
    """
    Orchestrates the GST data fill process.

    Handles:
    - Graceful Ctrl+C shutdown
    - Parallel GSTIN scraping with adaptive rate limiting
    - DataFrame filling from cached data
    """

    def __init__(self, config: FillConfig):
        self.config = config
        self.shutdown_requested = False
        self.cache_lock = Lock()
        self.checkpoint_mgr = CheckpointManager(
            config.checkpoint_file, self.cache_lock
        )

        # Install signal handler
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.warning("\nShutdown requested. Saving progress...")
        self.shutdown_requested = True

    def scrape_gstins(
        self,
        unique_gstins: List[str],
        gst_service: GstDataService,
        rate_limiter: AdaptiveRateLimiter,
    ) -> List[tuple]:
        """Scrape GSTINs in parallel with adaptive rate limiting."""
        logger.info(f"Scraping {len(unique_gstins)} unique GSTINs...")

        results = []
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {
                executor.submit(
                    self._scrape_single, gstin, gst_service, rate_limiter
                ): gstin
                for gstin in unique_gstins
            }

            with tqdm(
                total=len(unique_gstins), desc="Scraping GSTINs", unit="GSTIN"
            ) as pbar:
                batch_count = 0
                for future in as_completed(futures):
                    if self.shutdown_requested:
                        logger.info("Cancelling remaining tasks...")
                        gst_service.shutdown()
                        break

                    gstin = futures[future]
                    try:
                        data, had_429 = future.result()
                        results.append((gstin, data))

                        if had_429:
                            rate_limiter.record_429()
                            pause = rate_limiter.should_pause()
                            if pause > 0:
                                time.sleep(pause)
                                rate_limiter.consecutive_429s = 0
                        else:
                            rate_limiter.record_success()

                        batch_count += 1
                        if batch_count >= self.config.batch_size:
                            self.checkpoint_mgr.save(gst_service.cache)
                            batch_count = 0

                    except Exception as e:
                        logger.error(f"Error processing {gstin}: {e}")

                    pbar.update(1)

        self.checkpoint_mgr.save(gst_service.cache)
        return results

    @staticmethod
    def _scrape_single(
        gstin: str,
        gst_service: GstDataService,
        rate_limiter: AdaptiveRateLimiter,
    ) -> tuple:
        """Scrape a single GSTIN with adaptive rate limiting."""
        delay = rate_limiter.get_delay()
        time.sleep(delay)

        had_429 = False
        try:
            data = gst_service.get_gst_data(gstin)
            return data, had_429
        except Exception as e:
            if "429" in str(e) or "Too many requests" in str(e):
                had_429 = True
            raise

    @staticmethod
    def fill_dataframe(
        df: pd.DataFrame,
        gst_service: GstDataService,
        preloaded_cache: Optional[Dict[str, Any]] = None,
        name_map: Optional[Dict[str, Dict]] = None,
    ) -> pd.DataFrame:
        """Fill all rows in the DataFrame using cached GST data."""
        logger.info("Filling DataFrame from cache...")
        preloaded_cache = preloaded_cache or {}
        name_map = name_map or {}

        for col in GST_DATA_COLUMNS:
            if col not in df.columns:
                df[col] = None

        filled_count = 0
        for index, row in tqdm(
            df.iterrows(), total=len(df), desc="Filling rows", unit="row"
        ):
            gstin = (
                str(row.get("GSTIN", "")).strip()
                if pd.notna(row.get("GSTIN"))
                else None
            )
            if not gstin or gstin.lower() == "nan" or gstin == "":
                continue

            # Try preloaded cache first, then service cache
            data = preloaded_cache.get(gstin)
            if not data:
                data = gst_service.get_gst_data(gstin)

            if not data:
                continue

            # Fill missing Customer Name
            if pd.isna(row.get("Customer Name")) or not str(
                row.get("Customer Name", "")
            ).strip():
                df.at[index, "Customer Name"] = data.get("Legal Name", "N/A")

            # Fill missing Address
            if pd.isna(row.get("Address")) or not str(
                row.get("Address", "")
            ).strip():
                df.at[index, "Address"] = data.get("Principal Place", "N/A")

            # Fill Type
            if pd.isna(row.get("Type")) or not str(
                row.get("Type", "")
            ).strip():
                df.at[index, "Type"] = data.get("Constitution", "N/A")

            # Fill Customer Group & Remark via name map fallback
            if pd.isna(row.get("Customer Group")) or not str(
                row.get("Customer Group", "")
            ).strip():
                cg_found = False
                rm_found = False

                if pd.notna(data.get("Customer Group")):
                    df.at[index, "Customer Group"] = data["Customer Group"]
                    cg_found = True
                if pd.notna(data.get("Remark")):
                    df.at[index, "Remark"] = data["Remark"]
                    rm_found = True

                if not cg_found or not rm_found:
                    names_to_check = [
                        (
                            str(row.get("Customer Name")).strip().upper()
                            if pd.notna(row.get("Customer Name"))
                            else None
                        ),
                        (
                            str(data.get("Legal Name")).strip().upper()
                            if pd.notna(data.get("Legal Name"))
                            else None
                        ),
                        (
                            str(data.get("Trade Name")).strip().upper()
                            if pd.notna(data.get("Trade Name"))
                            else None
                        ),
                    ]
                    for n in names_to_check:
                        if n and n in name_map:
                            if not cg_found and name_map[n]["Customer Group"]:
                                df.at[index, "Customer Group"] = name_map[n][
                                    "Customer Group"
                                ]
                                cg_found = True
                            if not rm_found and name_map[n]["Remark"]:
                                df.at[index, "Remark"] = name_map[n]["Remark"]
                                rm_found = True
                            if cg_found and rm_found:
                                break

            # Fill all GST data columns
            for col in GST_DATA_COLUMNS:
                df.at[index, col] = data.get(col, "N/A")

            filled_count += 1

        logger.info(f"Filled {filled_count} rows")
        return df
