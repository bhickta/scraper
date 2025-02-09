from core.services.pdf_service import PDFService
from core.services.mcq_service import MCQExtractor
from core import re


class VisionMCQExtractor(MCQExtractor):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.questions = {}
        self.explanations = {}
        self.mcqs = []

    def process_questions(self):
        mcq_pattern = re.compile(
            r"^\s*(\d+)\.\s*\n(.*?)(?=\n\s*\d+\.\s*\n|\Z)", re.DOTALL | re.MULTILINE)
        last_question_no = 0

        for mcq in mcq_pattern.finditer(self.text):
            question_no = int(mcq.group(1))
            if last_question_no in [21]:
                print(mcq.group(2))
            if question_no != last_question_no + 1 and last_question_no != 0:
                continue

            question_text = mcq.group(2).strip()
            if "Copyright © by Vision IAS" in question_text:
                question_text = question_text.split(
                    "Copyright © by Vision IAS")[0].strip()
            options = self._extract_options(question_text)
            question_text = self._remove_options_from_text(
                question_text).strip()
            self.questions[question_no] = {
                "question": question_text,
            }
            for idx, option in enumerate(options):
                self.questions[question_no][chr(ord('a') + idx)] = option
            last_question_no = question_no

    def process_explanation(self):
        explanation_pattern = re.compile(
            r"^Q\s*(\d+)\.([A-F | a-f])", re.DOTALL | re.MULTILINE)
        last_question_no = 0

        for match in explanation_pattern.finditer(self.text):
            question_no = int(match.group(1))
            if question_no != last_question_no + 1 and last_question_no != 0:
                continue

            explanation_content = self._get_explanation_content(match)
            self.explanations[question_no] = {
                "answer": match.group(2).lower(),
                "explanation": explanation_content
            }
            last_question_no = question_no

    def _extract_options(self, question_text):
        options_pattern = re.compile(r"^\([a-f]\)\s*(.+)$", re.MULTILINE)
        return options_pattern.findall(question_text)

    def _remove_options_from_text(self, question_text):
        options_pattern = re.compile(r"^[a-f]\)\s*(.+)$", re.MULTILINE)
        return options_pattern.sub("", question_text)

    def _get_explanation_content(self, match):
        start = match.end()
        next_match = next(self._find_next_explanation(match), None)
        end = start + (next_match.start() if next_match else len(self.text))
        return self.text[start:end].strip()

    def _find_next_explanation(self, match):
        explanation_pattern = re.compile(
            r"^Q\s*(\d+)\.([A-F | a-f])", re.DOTALL | re.MULTILINE)
        return explanation_pattern.finditer(self.text[match.end():])

    def get_mcqs(self):
        self.mcqs = [
            {
                "question": question["question"],
                "a": question.get("a", ""),
                "answer": self.explanations.get(question_no, {}).get('answer'),
                "explanation": self.explanations.get(question_no, {}).get('explanation')
            }
            for question_no, question in self.questions.items()
        ]

    def run(self, **kwargs):
        self.text = self.pdf_service.extract_text(**kwargs)
        self.process_questions()
        self.process_explanation()
        self.get_mcqs()


if __name__ == "__main__":
    pdf_service = PDFService("./data/VISION IAS PRELIMS-2024 _TEST- 01-32.PDF")
    extractor = VisionMCQExtractor(
        pdf_service=pdf_service, output_path="vision_mcqs.json")
    extractor.run(pages=range(1, 59))
    extractor.to_json()
