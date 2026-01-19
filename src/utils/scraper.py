import re
from core import (
    UserAgent,
    retry,
    stop_after_attempt,
    wait_exponential,
    RetryError,
    logger,
    requests,
    BeautifulSoup,
    re,
    time,
    random,
    json
)


class Scraper:
    def __init__(self, **kwargs):
        self.base_url = kwargs.get('base_url', '').strip()
        self.content = kwargs.get('content')
        self.ua = UserAgent()
        self.session = requests.Session()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=5, max=30), reraise=True)
    def fetch_page(self):
        headers = {
            "User-Agent": self.ua.random,
            "Accept-Language": "en-US,en;q=0.9"
        }
        self.session.headers.update(headers)

        delay = random.uniform(2, 5)
        logger.info(
            f"Waiting {delay:.2f} seconds before request to {self.base_url}")
        time.sleep(delay)

        response = self.session.get(self.base_url, timeout=10)

        if response.status_code == 404:
            logger.warning(f"Page not found: {self.base_url} (404)")
            return ""

        if response.status_code == 429:
            logger.warning(f"Rate limit hit! Retrying... ({self.base_url})")
            raise Exception(f"Too many requests: {self.base_url} (429)")

        if response.status_code != 200:
            logger.error(
                f"Failed to fetch {self.base_url} (Status Code: {response.status_code})")
            raise Exception(
                f"Failed to fetch {self.base_url} (Status Code: {response.status_code})")

        return response.text

    def pre_parse(self, html_content):
        self.soup = BeautifulSoup(html_content, "html.parser")

    def scrape(self, content=None):
        try:
            html_content = self.content or content or self.fetch_page()
            self.pre_parse(html_content)
            return self.parse_page()
        except RetryError as e:
            logger.error(
                f"Retries failed for {self.base_url}. Error: {str(e)}")
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
        parts.append(r"Solution\s*[:\-\n]*")
        parts.append(r"Correct\s*Option[:\-]?\s*")
        parts.append(r"Correct\s*[:\-]?\s*")
        parts.append(r"Answeer\s*[:\-]?\s*")
        parts.append(r"Sol[:\.\-]\s*")  # Handling Sol. format
        parts.append(r"उत्तर\s*[:\-]?\s*")
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

    def normalize_whitespace(self, text):
        return re.sub(r'(\s)\1+', r'\1', text).strip()

    def extract_correct_answers(self):
        script_tag = self.soup.find('script', type='text/javascript',
                                    string=lambda text: "wpProQuizInitList" in text if text else False)

        if script_tag:
            script_content = script_tag.string
            start = script_content.find("json:") + len("json:")
            end = script_content.find("}}", start) + 2

            json_str = script_content[start:end]

            try:
                data = json.loads(json_str)
                correct_answers = []
                for question_id, details in data.items():
                    correct_index = details['correct'].index(1)
                    correct_answers.append(correct_index)
                return correct_answers

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                print("Raw JSON String:", json_str)
                return None  # Return None to indicate an error

        else:
            print("Script tag not found.")
            return None  # Return None if the script tag is not found

    def get_questions(self):
        quiz_list_items = self.soup.select('.wpProQuiz_listItem')
        questions = []
        option_labels = ["a", "b", "c", "d", "e", "f"]
        correct_answers = self.extract_correct_answers()
        correct_answer_map = {0: "a", 1: "b", 2: "c", 3: "d", 4: "e", 5: "f"}

        if correct_answers:
            for idx, no in enumerate(correct_answers):
                correct_answers[idx] = correct_answer_map[no]
        else:
            correct_answers = ["f", "f", "f", "f", "f", "f"]

        for index, item in enumerate(quiz_list_items):
            question = self.normalize_whitespace(
                item.select_one('.wpProQuiz_question_text').text)
            options = [self.normalize_whitespace(itm.text) for itm in item.select(
                '.wpProQuiz_questionListItem')]
            explanation = self.normalize_whitespace(
                item.select_one('.wpProQuiz_correct').text)
            answer = correct_answers[index]
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
