"""
Extract Unique GSTINs — scans all Excel files in the data directory and aggregates
all unique GSTINs into a single output file.
"""

import os
import pandas as pd
from typing import Set

# Configuration
DATA_DIR = "data"
OUTPUT_FILE = "data/all_unique_gstins.xlsx"

def extract_unique_gstins():
    """Scan all Excel files in DATA_DIR and collect unique GSTINs."""
    unique_gstins: Set[str] = set()
    files_to_process = []

    # 1. List all Excel files recursively
    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            if file.endswith((".xlsx", ".xls")) and not file.startswith("~$"):
                files_to_process.append(os.path.join(root, file))

    print(f"📂 Found {len(files_to_process)} Excel files to scan.")

    # 2. Extract GSTINs from each file
    for filepath in files_to_process:
        print(f"🔍 Scanning {filepath}...")
        try:
            # Load the Excel file (all sheets if necessary)
            xl = pd.ExcelFile(filepath)
            for sheet_name in xl.sheet_names:
                df = pd.read_excel(xl, sheet_name=sheet_name)
                
                # Find GSTIN column (case-insensitive)
                gst_col = next((c for c in df.columns if "gstin" in str(c).lower()), None)
                
                if gst_col:
                    # Collect all non-null GSTINs
                    extracted = df[gst_col].dropna().astype(str).str.strip()
                    extracted = extracted[extracted != ""].unique()
                    unique_gstins.update(extracted)
        except Exception as e:
            print(f"⚠️ Error reading {filepath}: {e}")

    # 3. Save to output
    if unique_gstins:
        print(f"\n📊 Total Unique GSTINs found: {len(unique_gstins)}")
        
        # Sort for better readability
        sorted_gstins = sorted(list(unique_gstins))
        
        df_output = pd.DataFrame({"GSTIN": sorted_gstins})
        df_output.to_excel(OUTPUT_FILE, index=False)
        print(f"💾 Unique GSTINs saved to {OUTPUT_FILE}")
    else:
        print("❌ No GSTINs found in any of the scanned files.")

if __name__ == "__main__":
    extract_unique_gstins()
