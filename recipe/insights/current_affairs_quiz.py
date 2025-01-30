from utils.scraper import SecureQuizUrl
import csv

def main():
    urls = ['https://www.insightsonindia.com/current-affairs-quiz/']
    get_url_csv(urls)

def get_url(start, end):
    with open('data/insights_ca_quiz_urls.csv', mode="r", newline='', encoding="utf-8") as file:
        return file.readlines()[start:end]

def get_url_csv(url):
    try:
        urls = []
        scraper = SecureQuizUrl(base_urls=url)
        scraper.scrape()
        print(scraper.urls)
    except Exception as e:
        print(e)
        raise e