import time
from tqdm import tqdm
from utils.scraper import SecureQuizUrl, MCQInsights, Scraper
import csv
from core.db import GenericDatabase, String

source = "current"
csv_file = f"./data/{source}.csv"
ouput_file = f"./data/{source}_outputs.csv"


def main():
    to_csv(ouput_file)
    return
    to_csv(ouput_file)
    return


def to_csv(output_file):
    db_path = f"sqlite:///data/{source}.db"
    db = GenericDatabase(db_path)

    with open(output_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow(
            ["question", "answer", "explanation", "a", "b", "c", "d", "e", "f", "source"])

        for url, html in db.get_urls_and_html("scraped_html"):
            scraper = MCQInsights(base_url=url)
            scraper.scrape(content=html)

            questions = scraper.scraped_data[0] if scraper.scraped_data else []

            if not questions:
                continue  # Skip if no questions found

            for question in questions:
                writer.writerow([
                    question.get("question", ""),
                    question.get("answer", ""),
                    question.get("explanation", ""),
                    question.get("a", ""),
                    question.get("b", ""),
                    question.get("c", ""),
                    question.get("d", ""),
                    question.get("e", ""),
                    question.get("f", ""),
                    url
                ])


def html_to_db():
    urls = get_url()
    db = GenericDatabase(f"sqlite:///data/{source}.db")
    db.create_table_if_not_exists(source, {"url": String, "html": String})

    total_urls = len(urls)
    start_time = time.time()

    with tqdm(total=total_urls, desc="Processing URLs", unit="url") as pbar:
        for index, url in enumerate(urls, start=1):
            # Update progress bar with estimated time left
            elapsed_time = time.time() - start_time
            avg_time_per_url = elapsed_time / index
            remaining_time = (avg_time_per_url * (total_urls - index)) / 3600

            pbar.set_postfix({"ETA": f"{remaining_time:.2f} hrs"})
            pbar.update(1)
            if db.url_exists(source, url):
                continue
            scraper = Scraper(base_url=url)
            scraper.scrape()
            html = scraper.get_html()
            db.insert(source, {"url": url, "html": html}, unique_field="url")


def get_url(start=None, end=None):
    """Fetch a range of URLs from the stored CSV file."""
    try:
        with open(csv_file, mode="r", newline="", encoding="utf-8") as file:
            lines = file.readlines()  # Read once and store

            # Set default values for start and end
            start = 0 if start is None else start - 1  # Convert to 0-based index
            # Ensure end is within range
            end = len(lines) if end is None else end

            print(f"Fetching lines {start + 1} to {end}")  # Debug output
            return lines[start:end]  # Return the sliced lines

    except FileNotFoundError:
        print("CSV file not found.")
        return []


def get_url_csv():
    urls = [
        # "https://www.insightsonindia.com/current-affairs-quiz/",
        # "https://www.insightsonindia.com/current-affairs-quiz/2/",
        # "https://www.insightsonindia.com/current-affairs-quiz/3/",
        # "https://www.insightsonindia.com/upsc-daily-static-quiz/",
        # "https://www.insightsonindia.com/upsc-daily-static-quiz/?lcp_page0=2#lcp_instance_0",
        # "https://www.insightsonindia.com/upsc-daily-static-quiz/?lcp_page0=3#lcp_instance_0",
        # "https://www.insightsonindia.com/upsc-daily-static-quiz/?lcp_page0=4#lcp_instance_0",
        # "https://www.insightsonindia.com/insights-current-affairs-revision-through-daily-mcqs/?lcp_page0=1#lcp_instance_0",
        # "https://www.insightsonindia.com/insights-current-affairs-revision-through-daily-mcqs/?lcp_page0=2#lcp_instance_0",
        # "https://www.insightsonindia.com/insights-current-affairs-revision-through-daily-mcqs/?lcp_page0=3#lcp_instance_0",
        # "https://www.insightsonindia.com/insights-current-affairs-revision-through-daily-mcqs/?lcp_page0=4#lcp_instance_0",
        # "https://www.insightsonindia.com/insta-dart/",
    ]
    scraped_urls = []
    existing_urls = set()  # Use a set for efficient checking of existing URLs

    # Load existing URLs from CSV if it exists
    try:
        with open(csv_file, mode="r", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader, None)  # Skip the header row if it exists
            for row in reader:
                existing_urls.add(row[0])  # Add existing URLs to the set
    except FileNotFoundError:
        pass  # If the file doesn't exist, start with an empty set

    try:
        for url in urls:
            scraper = SecureQuizUrl(base_url=url)
            scraper.scrape()
            if not scraper.urls:
                print(f"No URLs scraped from {url}")
                continue
            print(f"Scraped URLs from {url}")

            for scraped_url in scraper.urls:
                if (
                    scraped_url not in existing_urls
                ):  # check if url is already present or not
                    scraped_urls.append(scraped_url)
                    existing_urls.add(
                        scraped_url
                    )  # Add the new URL to the set to avoid duplicates in the current run

        if scraped_urls:
            with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow(["URL"])  # Add header
                for (
                    url
                    # Write all unique URLs (including existing ones)
                ) in existing_urls:
                    writer.writerow([url])
            print(f"URLs saved to {csv_file}")
        else:
            # If no new url is there to add
            print("No new unique URLs to add.")

    except Exception as e:
        print(f"Error: {e}")
        raise e
