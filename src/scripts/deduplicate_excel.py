"""
Deduplicate Excel by GSTIN

This utility removes duplicate GSTINs from an Excel file, keeping the first occurrence.
Useful for cleaning up the filled data.
"""

import pandas as pd
import logging
import sys
from datetime import datetime
import shutil

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def deduplicate_excel(input_file, output_file=None, backup=True):
    """
    Deduplicate Excel file by GSTIN.
    
    Args:
        input_file: Path to input Excel file
        output_file: Path to output file (default: overwrites input)
        backup: Whether to create backup before deduplication
    """
    logger.info(f"📂 Loading {input_file}...")
    df = pd.read_excel(input_file)
    original_count = len(df)
    logger.info(f"✓ Loaded {original_count} rows")
    
    # Create backup if requested
    if backup:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = input_file.replace('.xlsx', f'_before_dedup_{timestamp}.xlsx')
        try:
            shutil.copy2(input_file, backup_file)
            logger.info(f"✓ Backup created: {backup_file}")
        except Exception as e:
            logger.warning(f"Could not create backup: {e}")
    
    # Check for GSTIN column
    if 'GSTIN' not in df.columns:
        logger.error("❌ No 'GSTIN' column found in Excel file!")
        return
    
    # Find duplicates
    duplicates = df[df.duplicated(subset=['GSTIN'], keep='first')]
    duplicate_count = len(duplicates)
    
    if duplicate_count > 0:
        logger.info(f"🔍 Found {duplicate_count} duplicate GSTINs:")
        
        # Show duplicate GSTINs
        duplicate_gstins = duplicates['GSTIN'].value_counts()
        for gstin, count in duplicate_gstins.head(10).items():
            logger.info(f"  - {gstin}: appears {count + 1} times")
        
        if len(duplicate_gstins) > 10:
            logger.info(f"  ... and {len(duplicate_gstins) - 10} more")
        
        # Remove duplicates (keep first)
        df_deduped = df.drop_duplicates(subset=['GSTIN'], keep='first')
        logger.info(f"✓ Removed {duplicate_count} duplicates (kept first occurrence)")
        
        # Save
        output_path = output_file or input_file
        logger.info(f"💾 Saving {len(df_deduped)} unique rows to {output_path}...")
        df_deduped.to_excel(output_path, index=False)
        logger.info(f"✅ Done! Reduced from {original_count} to {len(df_deduped)} rows")
        
    else:
        logger.info("✓ No duplicate GSTINs found - file is already clean!")

def main():
    """Main execution."""
    if len(sys.argv) < 2:
        print("Usage: python deduplicate_excel.py <input_file> [output_file]")
        print("\nExamples:")
        print("  # Deduplicate and overwrite (creates backup)")
        print("  python deduplicate_excel.py data.xlsx")
        print("\n  # Deduplicate to new file")
        print("  python deduplicate_excel.py data.xlsx data_deduped.xlsx")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    deduplicate_excel(input_file, output_file, backup=True)

if __name__ == "__main__":
    main()
