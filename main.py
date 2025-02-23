import time
import sys
from recipe.arihant import main, subjects


def run_script():
    retries = 1
    for attempt in range(1, retries + 1):
        try:
            for subject, page_range in subjects():
                subject = subject + " : " + str(page_range)
                print(f"Running script for {subject}")
                main(pdf_path="./data/arihant/arihantMCQ.pdf",
                     output_path=f"./data/arihant/output/{subject}.json", pages=page_range, subject=subject)
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
