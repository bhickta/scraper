from recipe.insights import main
import time
import sys


def run_script():
    retries = 5
    for attempt in range(1, retries + 1):
        try:
            main()  # Attempt to run your main script logic
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
