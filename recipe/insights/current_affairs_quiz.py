from utils.scraper import SecureQuizUrl, MCQInsights
import csv

csv_file = "./data/insta_dart.csv"


def main():
    get_url_csv()
    return

    # Loop over the range from 1 to 4000, stepping by 50 to create pairs (1, 2), (50, 51), (100, 101), ...
    for start in range(1, 4001, 50):
        end = start + 1  # The end index is always 1 greater than the start index
        # Fetch the URLs from the CSV based on the current start and end
        urls = get_url(start, end)

        for url in urls:  # Process each URL fetched
            scraper = MCQInsights(base_url=url)
            scraper.scrape()
            # Check if there's any scraped data and print the answer
            if scraper.scraped_data:
                print(scraper.scraped_data[0][0].get("answer", "No answer found"))
            else:
                print(f"No data scraped from {url}")


def get_url(start, end):
    """Fetch a range of URLs from the stored CSV file."""
    try:
        with open(csv_file, mode="r", newline="", encoding="utf-8") as file:
            return file.readlines()[
                start:end
            ]  # Read the lines between start and end indices
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
        "https://www.insightsonindia.com/insta-dart/",
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
                ) in existing_urls:  # Write all unique URLs (including existing ones)
                    writer.writerow([url])
            print(f"URLs saved to {csv_file}")
        else:
            print("No new unique URLs to add.")  # If no new url is there to add

    except Exception as e:
        print(f"Error: {e}")
        raise e
