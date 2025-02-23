from core.services.pdf_service import PDFService
from core.services.mcq_service import MCQExtractor
from core import re


class GenericMCQExtractor(MCQExtractor):
    def __init__(self, pdf_service, **kwargs):
        super().__init__(**kwargs)
        self.pdf_service = pdf_service
        self.questions = {}
        self.explanations = {}
        self.mcqs = []
        self.text = ""

    def get_mcq_pattern(self):
        """Override this method to change how MCQs are detected."""
        return re.compile(r"^\s*(\d+)\.\s*\n(.*?)(?=\n\s*\d+\.\s*\n|\Z)", re.DOTALL | re.MULTILINE)

    def get_explanation_pattern(self):
        """Override this method to change how explanations are detected."""
        return re.compile(r"^Q\s*(\d+)\.([A-F | a-f])", re.DOTALL | re.MULTILINE)

    def get_options_pattern(self):
        """Override this method for different option formats."""
        return re.compile(r"^\([a-f]\)\s*(.+)$", re.MULTILINE)

    def process_questions(self):
        mcq_pattern = self.get_mcq_pattern()
        last_question_no = 0
        mcqs = mcq_pattern.findall(self.text)

        for mcq in mcqs:
            question_no = int(mcq[0])
            if question_no != last_question_no + 1 and last_question_no != 0:
                continue

            question_text = mcq[1]
            options = self._extract_options(question_text)
            question_text = self._remove_options_from_text(
                question_text).strip()

            self.questions[question_no] = {
                "question": question_text, "question_no": question_no}
            for idx, option in enumerate(options):
                self.questions[question_no][chr(ord('a') + idx)] = option

            last_question_no = question_no

    def process_explanation(self):
        explanation_pattern = self.get_explanation_pattern()
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
        options_pattern = self.get_options_pattern()
        return options_pattern.findall(question_text)

    def _remove_options_from_text(self, question_text):
        options_pattern = self.get_options_pattern()
        return options_pattern.sub("", question_text)

    def _get_explanation_content(self, match):
        start = match.end()
        next_match = next(self._find_next_explanation(match), None)
        end = start + (next_match.start() if next_match else len(self.text))
        return self.text[start:end].strip()

    def _find_next_explanation(self, match):
        explanation_pattern = self.get_explanation_pattern()
        return explanation_pattern.finditer(self.text[match.end():])

    def get_mcqs(self, source_name):
        self.mcqs = [
            {
                "question_no": question["question_no"],
                "question": question["question"],
                "a": question.get("a", ""),
                "b": question.get("b", ""),
                "c": question.get("c", ""),
                "d": question.get("d", ""),
                "source": source_name,
                "answer": self.explanations.get(question_no, {}).get('answer'),
                "explanation": self.explanations.get(question_no, {}).get('explanation')
            }
            for question_no, question in self.questions.items()
        ]

    def run(self, source_name, **kwargs):
        self.text = self.pdf_service.extract_text(**kwargs)
        self.process_questions()
        self.process_explanation()
        self.get_mcqs(source_name)


def main(pdf_path, output_path, pages, source_name):
    pdf_service = PDFService(pdf_path)
    extractor = GenericMCQExtractor(
        pdf_service=pdf_service, output_path=output_path)
    extractor.run(source_name, pages=pages)
    extractor.to_csv()
