
from utils.scraper import QuestionInsights
import csv

def main():
    def get_url(start, end):
        with open('data/secure_dec2020-dec2024.csv', mode="r", newline='', encoding="utf-8") as file:
            return file.readlines()[start:end]

    def push_questions(url):
        try:
            scraper = QuestionInsights(base_url=url)
            questions = scraper.scrape()
            with open('data/questions.csv', mode="a", newline='', encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow(["question", "link"])
                for question in questions:
                    writer.writerow([question.get("question"), question.get("link")])
        except Exception as e:
            print(e)
            raise e
    
    for url in get_url(1302, 1310):
        push_questions(url)