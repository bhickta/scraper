from utils.scraper import SecureQuizUrl, MCQInsights
import csv


def main():
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
        with open("data/insights_ca_quiz_urls.csv", mode="r", newline="", encoding="utf-8") as file:
            return file.readlines()[start:end]  # Read the lines between start and end indices
    except FileNotFoundError:
        print("CSV file not found.")
        return []


def get_url_csv():
    urls = [
        "https://www.insightsonindia.com/current-affairs-quiz/",
        "https://www.insightsonindia.com/current-affairs-quiz/2/",
        "https://www.insightsonindia.com/current-affairs-quiz/3/",
    ]
    get_url_csv(urls)
    scraped_urls = []

    try:
        for url in urls:
            scraper = SecureQuizUrl(base_url=url)  # Pass a single URL
            scraper.scrape()

            if not scraper.urls:
                print(f"No URLs scraped from {url}")
                continue

            print(f"Scraped URLs from {url}")
            scraped_urls.extend(scraper.urls)

        if scraped_urls:
            with open(
                "data/insights_ca_quiz_urls.csv", mode="w", newline="", encoding="utf-8"
            ) as file:
                writer = csv.writer(file)
                writer.writerow(["URL"])  # Add header
                for url in scraped_urls:
                    writer.writerow([url])

            print("URLs saved to insights_ca_quiz_urls.csv")

    except Exception as e:
        print(f"Error: {e}")
        raise e
