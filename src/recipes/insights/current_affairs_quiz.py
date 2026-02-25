"""
Insights Current Affairs Quiz — scrape and process quiz data.

Fetches quiz URLs, stores HTML in a SQLite database, and extracts
MCQ data for export.
"""

import csv
import time

from tqdm import tqdm

from src.core.base_scraper import BaseScraper
from src.core.db import GenericDatabase
from src.recipes.insights import MCQInsights, SecureQuizUrl

source = "current"
csv_file = f"./data/{source}.csv"
output_file = f"./data/{source}_outputs.csv"


def main():
    to_csv(output_file)


def to_csv(output_file_path: str) -> None:
    db_path = f"sqlite:///data/{source}.db"
    db = GenericDatabase(db_path)

    with open(output_file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([
            "question", "answer", "explanation",
            "a", "b", "c", "d", "e", "f", "source",
        ])

        for url, html in db.get_urls_and_html("scraped_html"):
            scraper = MCQInsights(base_url=url)
            scraper.scrape(content=html)

            questions = scraper.scraped_data[0] if scraper.scraped_data else []
            if not questions:
                continue

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
                    url,
                ])


def html_to_db() -> None:
    urls = get_url()
    from sqlalchemy import String

    db = GenericDatabase(f"sqlite:///data/{source}.db")
    db.create_table_if_not_exists(source, {"url": String, "html": String})

    total_urls = len(urls)
    start_time = time.time()

    with tqdm(total=total_urls, desc="Processing URLs", unit="url") as pbar:
        for index, url in enumerate(urls, start=1):
            elapsed_time = time.time() - start_time
            avg_time_per_url = elapsed_time / index
            remaining_time = (avg_time_per_url * (total_urls - index)) / 3600

            pbar.set_postfix({"ETA": f"{remaining_time:.2f} hrs"})
            pbar.update(1)
            if db.url_exists(source, url):
                continue
            scraper = BaseScraper(base_url=url)
            scraper.scrape()
            html = scraper.get_html()
            db.insert(source, {"url": url, "html": html}, unique_field="url")


def get_url(start=None, end=None) -> list:
    """Fetch a range of URLs from the stored CSV file."""
    try:
        with open(csv_file, mode="r", newline="", encoding="utf-8") as file:
            lines = file.readlines()
            start = 0 if start is None else start - 1
            end = len(lines) if end is None else end
            return lines[start:end]
    except FileNotFoundError:
        print("CSV file not found.")
        return []


def get_url_csv() -> None:
    urls = []  # Add URLs to scrape here
    scraped_urls = []
    existing_urls = set()

    try:
        with open(csv_file, mode="r", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader, None)
            for row in reader:
                existing_urls.add(row[0])
    except FileNotFoundError:
        pass

    try:
        for url in urls:
            scraper = SecureQuizUrl(base_url=url)
            scraper.scrape()
            if not scraper.urls:
                print(f"No URLs scraped from {url}")
                continue
            print(f"Scraped URLs from {url}")
            for scraped_url in scraper.urls:
                if scraped_url not in existing_urls:
                    scraped_urls.append(scraped_url)
                    existing_urls.add(scraped_url)

        if scraped_urls:
            with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow(["URL"])
                for url in existing_urls:
                    writer.writerow([url])
            print(f"URLs saved to {csv_file}")
        else:
            print("No new unique URLs to add.")

    except Exception as e:
        print(f"Error: {e}")
        raise
