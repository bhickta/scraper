"""IAS Score recipe — topic scrapers."""

from src.core.base_scraper import BaseScraper


class MicroTopicsIasscoreUrls(BaseScraper):
    """Extracts subject URLs from IAS Score."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.urls = []

    def parse_page(self):
        subject_url = self.soup.select('li[class=""] > a')
        self.urls.extend(
            "https://iasscore.in" + url.get("href") for url in subject_url
        )


class MicroTopicsIasscore(BaseScraper):
    """Extracts micro-topics from IAS Score subject pages."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.topics = []

    def parse_page(self):
        self.topics = []
        subject, section = [
            " ".join(t.split("-")).title()
            for t in self.base_url.split("/")[-2:]
        ]
        bricks = self.soup.select(".brick")
        for brick in bricks:
            topic = brick.select_one(".title").text.strip()
            themes = [li.text.strip() for li in brick.select(".sections ul li")]
            for theme in themes:
                key = {
                    "subject": subject,
                    "section": section,
                    "topic": topic,
                    "theme": theme,
                }
                hassubtheme = theme.split("\n\n")
                if len(hassubtheme) > 1:
                    key.update({
                        "theme": hassubtheme[0],
                        "subtheme": hassubtheme[1],
                    })
                self.topics.append(key)
