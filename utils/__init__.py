from utils.scraper import SecureInsightsUrl
import csv

def fetch_urls_insights_answer_writing():
    urls = []
    for i in range(1, 4):
        base_url = f"https://www.insightsonindia.com/upsc-mains-answer-writing-2025-insights-ias/{i}"
        scraper = SecureInsightsUrl(base_url=base_url)
        scraper.scrape()
        urls.extend(scraper.urls)

    with open('./insight_secure_urls.csv', mode="w", newline='', encoding="utf-8") as file:
        writer = csv.writer(file)
        for url in urls:
            writer.writerow([url])