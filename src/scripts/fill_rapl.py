"""
Optimized Rapl Filler - Pre-deduplication Strategy

This script first extracts unique GSTINs, scrapes them once, then fills all rows.
Much faster than row-by-row processing!
"""

import pandas as pd
import logging
import os
import sys
import json
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import signal

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))



# Configure logging - clean output
logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Silence all third-party library logs
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('src.core.base_scraper').setLevel(logging.CRITICAL)
logging.getLogger('src.services.gst_data_service').setLevel(logging.CRITICAL)

INPUT_FILE = "data/input/Estimated Data Rapl.xlsx"
OUTPUT_FILE = "data/input/Estimated Data Rapl_filled.xlsx"
CHECKPOINT_FILE = "data/input/.rapl_checkpoint_v2.json"

# Configuration
MAX_WORKERS = 3
BATCH_SIZE = 10

# Global shutdown flag
shutdown_requested = False
cache_lock = Lock()

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully."""
    global shutdown_requested
    logger.warning("\n⚠️  Shutdown requested. Saving progress...")
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)

def load_checkpoint():
    """Load checkpoint data if exists."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {"gstin_cache": {}}

def save_checkpoint(gstin_cache):
    """Save checkpoint data."""
    with cache_lock:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({"gstin_cache": gstin_cache}, f, indent=2)

def extract_unique_gstins(df):
    """Extract unique GSTINs from the dataframe."""
    unique_gstins = df['GSTIN'].dropna().astype(str).str.strip()
    unique_gstins = unique_gstins[unique_gstins != ''].unique()
    return list(unique_gstins)


from src.services.gst_data_service import GstDataService, AdaptiveRateLimiter


def scrape_unique_gstins(unique_gstins, gst_service, rate_limiter):
    """Scrape all unique GSTINs in parallel with adaptive rate limiting."""
    logger.info(f"📥 Scraping {len(unique_gstins)} unique GSTINs...")
    
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_with_rate_limit, gstin, gst_service, rate_limiter): gstin 
                   for gstin in unique_gstins}
        
        with tqdm(total=len(unique_gstins), desc="Scraping GSTINs", unit="GSTIN") as pbar:
            batch_count = 0
            for future in as_completed(futures):
                if shutdown_requested:
                    logger.info("Cancelling remaining tasks...")
                    gst_service.shutdown()
                    break
                
                gstin = futures[future]
                try:
                    data, had_429 = future.result()
                    results.append((gstin, data))
                    
                    # Update rate limiter
                    if had_429:
                        rate_limiter.record_429()
                        # Check if we need a long pause
                        pause = rate_limiter.should_pause()
                        if pause > 0:
                            time.sleep(pause)
                            rate_limiter.consecutive_429s = 0  # Reset after break
                    else:
                        rate_limiter.record_success()
                    
                    # Save checkpoint periodically
                    batch_count += 1
                    if batch_count >= BATCH_SIZE:
                        save_checkpoint(gst_service.cache)
                        batch_count = 0
                        
                except Exception as e:
                    logger.error(f"Error processing {gstin}: {e}")
                
                pbar.update(1)
    
    # Final checkpoint save
    save_checkpoint(gst_service.cache)
    return results

def scrape_with_rate_limit(gstin, gst_service, rate_limiter):
    """Scrape a single GSTIN with adaptive rate limiting."""
    # Apply adaptive delay before request
    delay = rate_limiter.get_delay()
    time.sleep(delay)
    
    # Track if we encountered 429
    had_429 = False
    
    try:
        data = gst_service.get_gst_data(gstin)
        return data, had_429
    except Exception as e:
        # Check if it's a rate limit error
        if '429' in str(e) or 'Too many requests' in str(e):
            had_429 = True
        raise

def fill_dataframe(df, gst_service):
    """Fill all rows using the cached GSTIN data."""
    logger.info("📝 Filling Excel rows from cache...")
    
    # Initialize new columns
    new_columns = [
        'Legal Name', 'Trade Name', 'Status', 'Registration Date',
        'City', 'District', 'State', 'Pincode',
        'E-Invoice Mandatory', 'Aggregate Turnover',
        'Central Jurisdiction', 'State Jurisdiction', 'HSN Codes'
    ]
    for col in new_columns:
        if col not in df.columns:
            df[col] = None
    
    filled_count = 0
    for index, row in tqdm(df.iterrows(), total=len(df), desc="Filling rows", unit="row"):
        gstin = str(row['GSTIN']).strip() if pd.notna(row['GSTIN']) else None
        
        if not gstin:
            continue
        
        # Get from cache (should be instant!)
        data = gst_service.get_gst_data(gstin)
        
        if data:
            # Fill missing Customer Name
            if pd.isna(row.get('Customer Name')) or str(row.get('Customer Name', '')).strip() == '':
                df.at[index, 'Customer Name'] = data.get('Legal Name', 'N/A')
            
            # Fill missing Address
            if pd.isna(row.get('Address')) or str(row.get('Address', '')).strip() == '':
                df.at[index, 'Address'] = data.get('Principal Place', 'N/A')
            
            # Fill Type and all new fields
            df.at[index, 'Type'] = data.get('Constitution', 'N/A')
            df.at[index, 'Legal Name'] = data.get('Legal Name', 'N/A')
            df.at[index, 'Trade Name'] = data.get('Trade Name', 'N/A')
            df.at[index, 'Status'] = data.get('Status', 'N/A')
            df.at[index, 'Registration Date'] = data.get('Registration Date', 'N/A')
            df.at[index, 'City'] = data.get('City', 'N/A')
            df.at[index, 'District'] = data.get('District', 'N/A')
            df.at[index, 'State'] = data.get('State', 'N/A')
            df.at[index, 'Pincode'] = data.get('Pincode', 'N/A')
            df.at[index, 'E-Invoice Mandatory'] = data.get('E-Invoice Mandatory', 'N/A')
            df.at[index, 'Aggregate Turnover'] = data.get('Aggregate Turnover', 'N/A')
            df.at[index, 'Central Jurisdiction'] = data.get('Central Jurisdiction', 'N/A')
            df.at[index, 'State Jurisdiction'] = data.get('State Jurisdiction', 'N/A')
            df.at[index, 'HSN Codes'] = data.get('HSN Codes', 'N/A')
            
            filled_count += 1
    
    logger.info(f"✓ Filled {filled_count} rows from cache")
    return df

def main():
    """Main execution flow."""
    import argparse
    parser = argparse.ArgumentParser(description="Optimized Rapl Data Filler")
    parser.add_argument("--retry-failed", action="store_true", help="Retry GSTINs that failed (marked as null) in previous runs")
    args = parser.parse_args()

    logger.info("🚀 Starting optimized Rapl data filler (pre-deduplication strategy)")
    
    # Create backup
    from datetime import datetime
    import shutil
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = INPUT_FILE.replace('.xlsx', f'_backup_{timestamp}.xlsx')
    try:
        shutil.copy2(INPUT_FILE, backup_file)
        logger.info(f"✓ Backup created: {backup_file}")
    except Exception as e:
        logger.warning(f"Could not create backup: {e}")
    
    # Load data
    logger.info(f"📂 Loading {INPUT_FILE}...")
    df = pd.read_excel(INPUT_FILE)
    logger.info(f"✓ Loaded {len(df)} rows")
    
    # Load checkpoint
    checkpoint = load_checkpoint()
    gstin_cache = checkpoint.get("gstin_cache", {})
    
    # Initialize service with cache (loaded from file)
    gst_service = GstDataService(cache=gstin_cache, cache_lock=cache_lock)
    
    # Initialize adaptive rate limiter
    rate_limiter = AdaptiveRateLimiter(base_delay=1.0, max_delay=10.0)
    logger.info("✓ Adaptive rate limiting enabled (1s-10s delays)")
    
    # Extract unique GSTINs
    unique_gstins = extract_unique_gstins(df)
    logger.info(f"✓ Found {len(unique_gstins)} unique GSTINs")
    
    # Filter: Which GSTINs need scraping?
    # 1. Not in cache at all (Missed ones)
    # 2. In cache but Null (Failed ones) IF retry is enabled
    gstins_to_scrape = []
    
    for gstin in unique_gstins:
        if gstin not in gstin_cache:
            gstins_to_scrape.append(gstin)
        elif args.retry_failed and gstin_cache[gstin] is None:
            gstins_to_scrape.append(gstin)
            # Remove from local cache memory so GstDataService will fetch it
            del gstin_cache[gstin]
    
    logger.info(f"✓ {len(gstin_cache)} valid cached, {len(gstins_to_scrape)} need scraping")
    if args.retry_failed:
        logger.info(f"  (Including retries for previously failed items)")

    # Scrape with adaptive rate limiting
    if gstins_to_scrape and not shutdown_requested:
        scrape_unique_gstins(gstins_to_scrape, gst_service, rate_limiter)
    
    # Show cache stats
    stats = gst_service.get_cache_stats()
    logger.info(f"📊 Cache: {stats['successful']} successful, {stats['failed']} failed, {stats['total_cached']} total")
    
    # Fill all rows from cache
    if not shutdown_requested:
        df = fill_dataframe(df, gst_service)
        
        # Deduplicate by GSTIN (keep first occurrence)
        logger.info("🔍 Checking for duplicate GSTINs...")
        original_count = len(df)
        df_deduped = df.drop_duplicates(subset=['GSTIN'], keep='first')
        duplicates_removed = original_count - len(df_deduped)
        
        if duplicates_removed > 0:
            logger.info(f"✓ Removed {duplicates_removed} duplicate GSTINs (kept first occurrence)")
            df = df_deduped
        else:
            logger.info("✓ No duplicate GSTINs found")
        
        # Save output
        logger.info(f"💾 Saving {len(df)} unique rows to {OUTPUT_FILE}...")
        df.to_excel(OUTPUT_FILE, index=False)
        logger.info("✅ All done!")
        
        # Clean up checkpoint
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
    else:
        logger.info("⚠️  Interrupted. Run again to resume from checkpoint.")

if __name__ == "__main__":
    main()
