import pandas as pd
import logging
import time
import os
import sys
import json
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import random
import signal

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.services.gst_data_service import GstDataService

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

INPUT_FILE = "data/input/Estimated Data Rapl.xlsx"
OUTPUT_FILE = "data/input/Estimated Data Rapl_filled.xlsx"
CHECKPOINT_FILE = "data/input/.rapl_checkpoint.json"

# Thread-safe locks
file_lock = Lock()
cache_lock = Lock()

# Configuration
MAX_WORKERS = 8  # Increased for 12 CPU system (keeping some headroom to avoid IP blocking)
BATCH_SIZE = 10  # Save to file after every N successful scrapes

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully."""
    global shutdown_requested
    logger.warning("\n⚠️  Shutdown requested. Saving progress and exiting gracefully...")
    shutdown_requested = True

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)

def load_checkpoint():
    """Load checkpoint data if exists."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {"processed_rows": [], "gstin_cache": {}}

def save_checkpoint(processed_rows, gstin_cache):
    """Save checkpoint data."""
    with cache_lock:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                "processed_rows": processed_rows,
                "gstin_cache": gstin_cache
            }, f)

def process_row(index, row, gst_service: GstDataService):
    """Process a single row using the GST data service."""
    if shutdown_requested:
        return {'index': index, 'success': False, 'data': None}
        
    result = {
        'index': index,
        'success': False,
        'data': None
    }
    
    gstin = str(row['GSTIN']).strip()
    if not gstin or pd.isna(row['GSTIN']):
        logger.warning(f"Row {index}: Missing GSTIN, skipping scraping.")
        return result
    
    # Use the decoupled GST data service
    data = gst_service.get_gst_data(gstin)
    
    if data:
        result['success'] = True
        result['data'] = {
            'Customer Name': data.get('Legal Name', 'N/A') if (pd.isna(row['Customer Name']) or str(row['Customer Name']).strip() == '') else None,
            'Trade Name Fallback': data.get('Trade Name', 'N/A'),
            'Address': data.get('Principal Place', 'N/A') if (pd.isna(row['Address']) or str(row['Address']).strip() == '') else None,
            'Type': data.get('Constitution', 'N/A'),
            'Legal Name': data.get('Legal Name', 'N/A'),
            'Trade Name': data.get('Trade Name', 'N/A'),
            'Status': data.get('Status', 'N/A'),
            'Registration Date': data.get('Registration Date', 'N/A'),
            'City': data.get('City', 'N/A'),
            'District': data.get('District', 'N/A'),
            'State': data.get('State', 'N/A'),
            'Pincode': data.get('Pincode', 'N/A'),
            'E-Invoice Mandatory': data.get('E-Invoice Mandatory', 'N/A'),
            'Aggregate Turnover': data.get('Aggregate Turnover', 'N/A'),
            'Central Jurisdiction': data.get('Central Jurisdiction', 'N/A'),
            'State Jurisdiction': data.get('State Jurisdiction', 'N/A'),
            'HSN Codes': data.get('HSN Codes', 'N/A'),
        }
    
    return result

def update_dataframe(df, results):
    """Update dataframe with results and save to file."""
    for result in results:
        if not result['success']:
            continue
            
        index = result['index']
        data = result['data']
        
        # Fill name if missing
        if data['Customer Name'] is not None:
            legal_name = data['Customer Name']
            trade_name = data['Trade Name Fallback']
            final_name = legal_name if legal_name != 'N/A' else trade_name
            df.at[index, 'Customer Name'] = final_name
        
        # Fill address if missing
        if data['Address'] is not None:
            df.at[index, 'Address'] = data['Address']
        
        # Update Type if it was N/A
        if df.at[index, 'Type'] == 'N/A':
            df.at[index, 'Type'] = data['Type']
        
        # Always populate structured fields (for ERPNext and additional data)
        df.at[index, 'Legal Name'] = data['Legal Name']
        df.at[index, 'Trade Name'] = data['Trade Name']
        df.at[index, 'Status'] = data['Status']
        df.at[index, 'Registration Date'] = data['Registration Date']
        df.at[index, 'City'] = data['City']
        df.at[index, 'District'] = data['District']
        df.at[index, 'State'] = data['State']
        df.at[index, 'Pincode'] = data['Pincode']
        df.at[index, 'E-Invoice Mandatory'] = data['E-Invoice Mandatory']
        df.at[index, 'Aggregate Turnover'] = data['Aggregate Turnover']
        df.at[index, 'Central Jurisdiction'] = data['Central Jurisdiction']
        df.at[index, 'State Jurisdiction'] = data['State Jurisdiction']
        df.at[index, 'HSN Codes'] = data['HSN Codes']
    
    # Save to file immediately
    with file_lock:
        df.to_excel(OUTPUT_FILE, index=False)

def fill_missing_data():
    if not os.path.exists(INPUT_FILE):
        logger.error(f"Input file not found: {INPUT_FILE}")
        return

    # Create backup before processing
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = INPUT_FILE.replace('.xlsx', f'_backup_{timestamp}.xlsx')
    
    try:
        import shutil
        shutil.copy2(INPUT_FILE, backup_file)
        logger.info(f"✓ Backup created: {backup_file}")
    except Exception as e:
        logger.error(f"Failed to create backup: {e}")
        response = input("Continue without backup? (yes/no): ")
        if response.lower() != 'yes':
            logger.info("Aborted by user.")
            return

    logger.info(f"Loading data from {INPUT_FILE}...")
    try:
        df = pd.read_excel(INPUT_FILE)
    except Exception as e:
        logger.error(f"Failed to read Excel file: {e}")
        return

    # Load checkpoint
    checkpoint = load_checkpoint()
    gstin_cache = checkpoint.get("gstin_cache", {})
    processed_rows = set(checkpoint.get("processed_rows", []))
    
    # Initialize the decoupled GST data service
    gst_service = GstDataService(cache=gstin_cache, cache_lock=cache_lock)

    # Initialize new columns if they don't exist
    new_columns = [
        'Legal Name', 'Trade Name', 'Status', 'Registration Date',
        'City', 'District', 'State', 'Pincode',
        'E-Invoice Mandatory', 'Aggregate Turnover',
        'Central Jurisdiction', 'State Jurisdiction', 'HSN Codes'
    ]
    for col in new_columns:
        if col not in df.columns:
            df[col] = None

    total_rows = len(df)
    logger.info(f"Total rows: {total_rows}")
    logger.info(f"Already processed: {len(processed_rows)} rows")

    # Count rows that need processing
    rows_to_process = []
    for index, row in df.iterrows():
        if index in processed_rows:
            continue
        
        # Fill defaults for other columns (convert to string to avoid dtype warning)
        if pd.isna(row.get('Code')): 
            df.at[index, 'Code'] = str('N/A')
        if pd.isna(row.get('Type')): 
            df.at[index, 'Type'] = str('N/A')
        if pd.isna(row.get('Remark')): 
            df.at[index, 'Remark'] = str('N/A')
        if pd.isna(row.get('Customer Group')): 
            df.at[index, 'Customer Group'] = str('N/A')
            
        # Check if we need to scrape this row
        name_missing = pd.isna(row.get('Customer Name')) or str(row.get('Customer Name', '')).strip() == ''
        address_missing = pd.isna(row.get('Address')) or str(row.get('Address', '')).strip() == ''
        
        # Check if structured fields are missing (for ERPNext)
        city_missing = pd.isna(row.get('City')) or str(row.get('City', '')).strip() == ''
        state_missing = pd.isna(row.get('State')) or str(row.get('State', '')).strip() == ''
        pincode_missing = pd.isna(row.get('Pincode')) or str(row.get('Pincode', '')).strip() == ''
        
        # Scrape if ANY critical field is missing
        if name_missing or address_missing or city_missing or state_missing or pincode_missing:
            rows_to_process.append((index, row))

    logger.info(f"Rows needing processing: {len(rows_to_process)}")
    
    if not rows_to_process:
        logger.info("No rows need processing. Saving file...")
        df.to_excel(OUTPUT_FILE, index=False)
        logger.info("Done.")
        return

    # Process rows in parallel with batching
    total_to_process = len(rows_to_process)
    batch_results = []
    
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_row, idx, row, gst_service): idx for idx, row in rows_to_process}
            
            with tqdm(total=total_to_process, desc="Processing rows", unit="row") as pbar:
                for future in as_completed(futures):
                    if shutdown_requested:
                        logger.info("Cancelling remaining tasks...")
                        gst_service.shutdown()
                        break
                        
                    result = future.result()
                    batch_results.append(result)
                    processed_rows.add(result['index'])
                    pbar.update(1)
                    
                    # Save batch and checkpoint
                    if len(batch_results) >= BATCH_SIZE:
                        update_dataframe(df, batch_results)
                        save_checkpoint(list(processed_rows), gstin_cache)
                        batch_results = []
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        gst_service.shutdown()
    
    # Save any remaining results
    if batch_results:
        logger.info("Saving remaining results...")
        update_dataframe(df, batch_results)
    
    # Log cache statistics
    stats = gst_service.get_cache_stats()
    logger.info(f"Cache stats: {stats['successful']} successful, {stats['failed']} failed, {stats['total_cached']} total")
    
    logger.info(f"Saving final data to {OUTPUT_FILE}...")
    with file_lock:
        df.to_excel(OUTPUT_FILE, index=False)
    save_checkpoint(list(processed_rows), gstin_cache)
    
    if shutdown_requested:
        logger.info(f"✓ Progress saved! Processed {len(processed_rows)} rows. Run again to resume.")
    else:
        # Clean up checkpoint file only if completed
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
        logger.info("✓ All done!")

if __name__ == "__main__":
    fill_missing_data()
