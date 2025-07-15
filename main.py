import time
import sys
from recipe.dggca import main
from recipe.gst import main as _gst_main


def run_script():
    retries = 1
    for attempt in range(1, retries + 1):
        try:
            main(pdf_path="./data/dggca/dggJan2025.pdf",
                 output_path="./data/dggca/output/dggJan2025.csv", pages=None, source="ddgJan2025")
            print("Script ran successfully. Shutting down.")
            sys.exit(0)  # Shut down if successful
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")
            if attempt == retries:
                print("Max retries reached. Exiting.")
                sys.exit(1)  # Exit if max retries reached
            time.sleep(2)  # Wait before retrying


if __name__ == "__main__":
    _gst_main(input_csv="data/input.csv",
              output_csv="gst_dump.csv")
