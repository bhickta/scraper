from core import (
    UserAgent,
    retry,
    stop_after_attempt,
    wait_fixed,
    RetryError,
    logger,
    requests,
    BeautifulSoup,
    re
)


class Scraper:
    def __init__(self, **kwargs):
        self.base_url = kwargs.get('base_url').strip(
        ) if kwargs.get('base_url') else None
        self.content = kwargs.get('content')
        self.ua = UserAgent()
        self.session = requests.Session()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
    def fetch_page(self):
        headers = {
            "User-Agent": self.ua.random,
        }
        self.session.headers.update(headers)
        response = self.session.get(self.base_url, timeout=10)

        if response.status_code != 200:
            logger.error(
                f"Failed to fetch the page: {
                    self.base_url} (Status Code: {response.status_code})"
            )
            raise Exception(
                f"Failed to fetch the page: {
                    self.base_url} (Status Code: {response.status_code})"
            )

        return response.text

    def pre_parse(self, html_content):
        self.soup = BeautifulSoup(html_content, "html.parser")

    def scrape(self, content=None):
        try:
            html_content = self.content or content or self.fetch_page()
            self.pre_parse(html_content)
            return self.parse_page()
        except RetryError as e:
            logger.error(f"Retries failed for {
                         self.base_url}. Error: {str(e)}")
            raise

    def parse_page(self):
        pass

    def get_html(self):
        return self.soup.prettify()


class MCQInsights(Scraper):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def parse_page(self):
        self.scraped_data = []
        questions = self.get_questions()
        self.scraped_data.append(questions)

    def build_answer_pattern(self):
        parts = []

        parts.append(r"Ans\s*[:\.\-]\s*")  # Ans., Ans:, Ans-
        parts.append(r"Ans-\.\s*")
        parts.append(r"Solution[:\-]\s*")  # Solution:
        parts.append(r"Correct\s*Option[:\-]?\s*")
        parts.append(r"Correct\s*[:\-]?\s*")
        parts.append(r"Answeer\s*[:\-]?\s*")
        parts.append(r"Sol[:\.\-]\s*")  # Handling Sol. format
        parts.append(r"Correct\s*Answer[:\-]?\s*")
        parts.append(r"Answer\s*[:\-]?\s*")  # Answer: or Answer -
        parts.append(r"Sol\s*[:\-]?\s*")  # Sol: or Sol -
        parts.append(r"SOLUTION[:\-]\s*")  # Handling SOLUTION: format
        parts.append(r"Ans[:\.\-]\s*(?:\d+\)\s*)?")  # Number is optional

        start_part = "|".join(parts)

        # Capture the option (in parentheses or without parentheses)
        option_part = r"\s*(?:\(?([a-dA-D])\)?)*"

        # Capture the remaining text (non-greedy)
        remaining_part = r"(.*)"

        regex = r"(" + start_part + r")" + option_part + \
            remaining_part  # Group everything

        return regex

    def build_answer_pattern(self):
        parts = []

        parts.append(r"Ans\s*[:\.\-]\s*")  # Ans., Ans:, Ans-
        parts.append(r"Ans-\.\s*")
        parts.append(r"Solution[:\-]\s*")  # Solution:
        parts.append(r"Correct\s*Option[:\-]?\s*")
        parts.append(r"Correct\s*[:\-]?\s*")
        parts.append(r"Answeer\s*[:\-]?\s*")
        parts.append(r"Sol[:\.\-]\s*")  # Handling Sol. format
        parts.append(r"Correct\s*Answer[:\-]?\s*")
        parts.append(r"Answer\s*[:\-]?\s*")  # Answer: or Answer -
        parts.append(r"Sol\s*[:\-]?\s*")  # Sol: or Sol -
        parts.append(r"SOLUTION[:\-]\s*")  # Handling SOLUTION: format
        parts.append(r"Ans[:\.\-]\s*(?:\d+\)\s*)?")  # Number is optional

        start_part = "|".join(parts)

        # Capture the option (in parentheses or without parentheses)
        option_part = r"\s*(?:\(?([a-dA-D])\)?)*"

        # Capture the remaining text (non-greedy)
        remaining_part = r"(.*)"

        regex = r"(" + start_part + r")" + option_part + \
            remaining_part  # Group everything

        return regex

    def get_questions(self):
        quiz_list_items = self.soup.select('.wpProQuiz_listItem')
        questions = []

        option_labels = ["a", "b", "c", "d", "e", "f"]
        for item in quiz_list_items:
            question = item.select_one('.wpProQuiz_question_text').text.strip()
            options = [itm.text.strip()
                       for itm in item.select('.wpProQuiz_questionListItem')]
            explaination = item.select_one(".wpProQuiz_response").text
            correct_answer_element = item.select_one(".wpProQuiz_correct p")
            answer = ""
            if correct_answer_element:
                correct_answer_text = correct_answer_element.text.strip()

                regex = self.build_answer_pattern()
                match = re.search(regex, correct_answer_text)

                if match and match.group(1):
                    answer_group = match.group(2)
                    if answer_group:
                        answer = answer_group.lower()

            ret = {
                "question": question,
                "answer": answer,
                "explaination": explaination,
            }

            for idx, option in enumerate(options):
                if idx < len(option_labels):
                    ret[option_labels[idx]] = option

            questions.append(ret)

        return questions


class QuestionInsights(Scraper):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def parse_page(self):
        scraped_data = []
        questions = self.get_questions()

        for question in questions:
            entry = {}
            entry["question"] = question.text
            entry["link"] = question.get("href")
            scraped_data.append(entry)
        return scraped_data

    def get_questions(self):
        # questions = self.soup.select("article .entry-content p:has(a strong)")
        questions = self.soup.select(".entry-content p a:has(span strong)")
        return questions


class SecureInsightsUrl(Scraper):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.urls = []

    def parse_page(self):
        month_url = self.soup.select(".entry-content a")
        month_url = [url.get("href") for url in month_url]
        self.urls.extend(month_url)


class SecureQuizUrl(Scraper):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.urls = []

    def parse_page(self):
        soup = self.soup.select_one(".row")
        month_url = soup.select(".entry-content a")
        month_url = [url.get("href") for url in month_url]
        self.urls.extend(month_url)


class MicroTopicsIasscoreUrls(Scraper):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.urls = []

    def parse_page(self):
        subject_url = self.soup.select('li[class=""] > a')
        subject_url = ["https://iasscore.in" +
                       url.get("href") for url in subject_url]
        self.urls.extend(subject_url)


class MicroTopicsIasscore(Scraper):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def parse_page(self):
        self.topics = []
        subject, section = [(" ").join(t.split("-")).title()
                            for t in self.base_url.split("/")[-2:]]
        bricks = self.soup.select('.brick')
        for brick in bricks:
            topic = brick.select_one('.title').text.strip()
            themes = [li.text.strip()
                      for li in brick.select('.sections ul li')]
            for theme in themes:
                key = {
                    "subject": subject,
                    "section": section,
                    "topic": topic,
                    "theme": theme
                }
                hassubtheme = theme.split("\n\n")
                if len(hassubtheme) > 1:
                    subtheme = hassubtheme[1]
                    theme = hassubtheme[0]
                    key.update({
                        "theme": theme,
                        "subtheme": subtheme
                    })
                self.topics.append(key)
