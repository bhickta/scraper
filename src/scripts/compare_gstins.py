"""
Compare GSTINs — compares the local unique GSTINs with a portal export CSV.
Lists GSTINs present locally but missing from the portal.
"""

import pandas as pd

# Configuration
LOCAL_FILE = "data/all_unique_gstins.xlsx"
PORTAL_FILE = "data/all_unique_gstins_portal.csv"
OUTPUT_FILE = "data/missing_from_portal.xlsx"

def compare_gstins():
    """Compare local vs portal GSTINs and save the difference."""
    try:
        # Load local unique GSTINs
        df_local = pd.read_excel(LOCAL_FILE)
        local_gstins = set(df_local["GSTIN"].dropna().astype(str).str.strip().unique())
        print(f"📊 Loaded {len(local_gstins)} local unique GSTINs.")

        # Load portal GSTINs
        # Portal file might not have a header or might have a different column name
        df_portal = pd.read_csv(PORTAL_FILE)
        
        # Identify GSTIN column (case-insensitive)
        gst_col = next((c for c in df_portal.columns if "gstin" in str(c).lower()), None)
        
        if not gst_col:
            # If no column name matches, try assuming the first column
            gst_col = df_portal.columns[0]
            print(f"⚠️ No 'GSTIN' column found in portal file, using first column: '{gst_col}'")
        
        portal_gstins = set(df_portal[gst_col].dropna().astype(str).str.strip().unique())
        print(f"🌍 Loaded {len(portal_gstins)} portal GSTINs.")

        # Finding the difference: (Local - Portal)
        missing_gstins = sorted(list(local_gstins - portal_gstins))
        
        print(f"\n✨ Found {len(missing_gstins)} GSTINs missing from portal.")

        if missing_gstins:
            df_missing = pd.DataFrame({"GSTIN": missing_gstins})
            df_missing.to_excel(OUTPUT_FILE, index=False)
            print(f"💾 Missing GSTINs saved to {OUTPUT_FILE}")
        else:
            print("✅ All local GSTINs are already in the portal file.")

    except Exception as e:
        print(f"❌ Error during comparison: {e}")

if __name__ == "__main__":
    compare_gstins()
