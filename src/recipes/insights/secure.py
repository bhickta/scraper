"""Insights Secure — extract questions from InsightsOnIndia Secure pages."""

import csv

from src.recipes.insights import QuestionInsights


def get_url(start: int, end: int) -> list:
    with open(
        "data/secure_dec2020-dec2024.csv", mode="r", newline="", encoding="utf-8"
    ) as file:
        return file.readlines()[start:end]


def push_questions(url: str) -> None:
    try:
        scraper = QuestionInsights(base_url=url)
        questions = scraper.scrape()
        with open(
            "data/questions.csv", mode="a", newline="", encoding="utf-8"
        ) as file:
            writer = csv.writer(file)
            writer.writerow(["question", "link"])
            for question in questions:
                writer.writerow([
                    question.get("question"),
                    question.get("link"),
                ])
    except Exception as e:
        print(e)
        raise