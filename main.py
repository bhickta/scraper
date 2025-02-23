import time
import sys
from recipe.vision import main


def run_script():
    retries = 5
    for attempt in range(1, retries + 1):
        try:
            # Attempt to run your main script logic
            main(pdf_path="./data/vision/GS_TEST_1_-_INDIAN_POLITY_AND_.pdf",
                 output_path="./data/vision/output/gs_test1.csv", pages=range(1, 20))
            print("Script ran successfully. Shutting down.")
            sys.exit(0)  # Shut down if successful
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")
            if attempt == retries:
                print("Max retries reached. Exiting.")
                sys.exit(1)  # Exit if max retries reached
            time.sleep(2)  # Wait before retrying


if __name__ == "__main__":
    run_script()
