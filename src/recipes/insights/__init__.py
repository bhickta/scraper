"""Insights recipe — scrapers for InsightsOnIndia quizzes and answer writing."""

from src.core.base_scraper import BaseScraper


class MCQInsights(BaseScraper):
    """Extracts MCQs from InsightsOnIndia quiz pages (wpProQuiz format)."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.scraped_data = []

    def parse_page(self):
        self.scraped_data = []
        questions = self._get_questions()
        self.scraped_data.append(questions)

    def _get_questions(self):
        import json

        quiz_list_items = self.soup.select(".wpProQuiz_listItem")
        questions = []
        option_labels = ["a", "b", "c", "d", "e", "f"]
        correct_answers = self._extract_correct_answers()
        correct_answer_map = {0: "a", 1: "b", 2: "c", 3: "d", 4: "e", 5: "f"}

        if correct_answers:
            for idx, no in enumerate(correct_answers):
                correct_answers[idx] = correct_answer_map[no]
        else:
            correct_answers = ["f"] * 6

        for index, item in enumerate(quiz_list_items):
            question = self.normalize_whitespace(
                item.select_one(".wpProQuiz_question_text").text
            )
            options = [
                self.normalize_whitespace(itm.text)
                for itm in item.select(".wpProQuiz_questionListItem")
            ]
            explanation = self.normalize_whitespace(
                item.select_one(".wpProQuiz_correct").text
            )
            answer = correct_answers[index] if index < len(correct_answers) else "f"
            ret = {
                "question": question,
                "answer": answer,
                "explanation": explanation,
            }
            for idx, option in enumerate(options):
                if idx < len(option_labels):
                    ret[option_labels[idx]] = option
            questions.append(ret)

        return questions

    def _extract_correct_answers(self):
        import json

        script_tag = self.soup.find(
            "script",
            type="text/javascript",
            string=lambda text: "wpProQuizInitList" in text if text else False,
        )
        if not script_tag:
            return None

        script_content = script_tag.string
        start = script_content.find("json:") + len("json:")
        end = script_content.find("}}", start) + 2
        json_str = script_content[start:end]

        try:
            data = json.loads(json_str)
            return [details["correct"].index(1) for details in data.values()]
        except json.JSONDecodeError:
            return None


class QuestionInsights(BaseScraper):
    """Extracts question links from InsightsOnIndia Secure/Mains pages."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def parse_page(self):
        scraped_data = []
        questions = self.soup.select(".entry-content p a:has(span strong)")
        for question in questions:
            scraped_data.append({
                "question": question.text,
                "link": question.get("href"),
            })
        return scraped_data


class SecureInsightsUrl(BaseScraper):
    """Extracts month-level URLs from InsightsOnIndia."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.urls = []

    def parse_page(self):
        month_url = self.soup.select(".entry-content a")
        self.urls.extend(url.get("href") for url in month_url)


class SecureQuizUrl(BaseScraper):
    """Extracts quiz URLs from InsightsOnIndia quiz listing pages."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.urls = []

    def parse_page(self):
        soup = self.soup.select_one(".row")
        month_url = soup.select(".entry-content a")
        self.urls.extend(url.get("href") for url in month_url)