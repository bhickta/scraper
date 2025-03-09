def run_script():
    # for arihant
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


# arihant
def run_script():
    retries = 1
    for attempt in range(1, retries + 1):
        try:
            main(pdf_path="./data/agriculture/PYQ Agriculture Planner 2025.pdf",
                 output_path="./data/agriculture/pyq_2025_krushna.csv", pages=None)
            print("Script ran successfully. Shutting down.")
            sys.exit(0)  # Shut down if successful
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")
            if attempt == retries:
                print("Max retries reached. Exiting.")
                sys.exit(1)  # Exit if max retries reached
            time.sleep(2)  # Wait before retrying
