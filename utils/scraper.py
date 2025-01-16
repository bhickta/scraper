from core import (
    UserAgent,
    retry,
    stop_after_attempt,
    wait_fixed,
    RetryError,
    logger,
    requests,
    BeautifulSoup,
)


class InsightScraper:
    def __init__(self, **kwargs):
        self.base_url = kwargs.get('base_url').strip()
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
                f"Failed to fetch the page: {self.base_url} (Status Code: {response.status_code})"
            )
            raise Exception(
                f"Failed to fetch the page: {self.base_url} (Status Code: {response.status_code})"
            )

        return response.text

    def pre_parse(self, html_content):
        self.soup = BeautifulSoup(html_content, "html.parser")

    def scrape(self):
        try:
            html_content = self.fetch_page()
            self.pre_parse(html_content)
            return self.parse_page()
        except RetryError as e:
            logger.error(f"Retries failed for {self.base_url}. Error: {str(e)}")
            raise


class QuestionInsights(InsightScraper):
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


class SecureInsightsUrl(InsightScraper):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.urls = []

    def parse_page(self):
        month_url = self.soup.select(".entry-content a")
        month_url = [url.get("href") for url in month_url]
        self.urls.extend(month_url)
