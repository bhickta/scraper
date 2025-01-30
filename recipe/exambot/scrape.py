from utils.scraper import Scraper
import json


class ExamBot(Scraper):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def parse_page(self):
        soup = self.soup.select_one("div.content-inner")
        data = {
            "subject": None,
            "question": None,
            "correct_option": None,
            "explaination": None,
            "options": None,
            "html": None,
        }
        if not soup:
            data.update({"html": str(self.soup)})
        else:
            explaination = soup.select_one("div.card.card-body")
            subject = soup.select_one("div.field-item.even")
            question_statement = soup.select_one("tbody tr td")
            radio_inputs = soup.select('input[type="radio"]')

            options = []
            correct_option = None

            for radio in radio_inputs:
                value = radio.get("value")
                id_attr = radio.get("id")
                onclick = radio.get("onclick")
                label = soup.find("label", {"for": id_attr})
                label_text = label.get_text(strip=True)

                args = [
                    arg.strip()
                    for arg in onclick.split("(")[1].split(")")[0].split(",")
                ]
                correct_option = args[-1]

                options.append({"value": value, "label": label_text})

            data.update(
                {
                    "subject": subject.text.strip() if subject else "",
                    "question": (
                        question_statement.text.strip() if question_statement else ""
                    ),
                    "correct_option": correct_option,
                    "explaination": explaination.text.strip() if explaination else "",
                    "options": options,
                }
            )

        return data
