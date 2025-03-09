import re
import unicodedata
from core.services.pdf_service import PDFService
from core import csv, os


class QuestionParser:
    def __init__(self, text, **kwargs):
        self.text = self.normalize_text(text)
        with open('input.txt', 'w', encoding='utf-8') as f:
            f.write(self.text)
        self.questions = []
        self.source = kwargs.get("source")

    def normalize_text(self, text):
        text = unicodedata.normalize("NFKC", text)  # Normalize Unicode
        # Remove hidden LTR/RTL markers
        text = re.sub(r'[\u202A-\u202E]', '', text)
        # Replace multiple spaces/tabs with a single space
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n+', '\n', text)  # Ensure single newlines
        # Add space between merged words
        text = re.sub(r'(\w)([A-Z])', r'\1 \2', text)
        # Fix misplaced spaces before punctuation
        text = text.replace(" .", ".").replace(" ,", ",")
        # Fix misplaced spaces around parentheses
        text = text.replace(" (", "(").replace(" )", ")")
        return text.strip()

    def parse(self):
        dates = self.split_dates(self.text)
        for date, content in dates:
            self.extract_questions(date, content)

    def split_dates(self, text):
        date_pattern = re.compile(
            r'^(\d{1,2}(?:st|nd|rd|th)\s+\w+)$', re.MULTILINE)
        matches = list(date_pattern.finditer(text))

        if not matches:
            return []

        dates_with_content = []
        for i in range(len(matches)):
            start_idx = matches[i].start()
            end_idx = matches[i + 1].start() if i + \
                1 < len(matches) else len(text)

            date = matches[i].group(1)
            content = text[start_idx:end_idx].strip()
            dates_with_content.append((date, content))

        return dates_with_content

    def extract_questions(self, date, content):
        question_pattern = re.compile(
            r'Q\)\s*(.*?)\s*'  # Question
            r'A\.\s*(.*?)\s*'  # Option A
            r'B\.\s*(.*?)\s*'  # Option B
            r'C\.\s*(.*?)\s*'  # Option C
            r'D\.\s*(.*?)\s*'  # Option D
            r'Answer:\s*([A-D])\.\s*(.*?)\s*'  # Correct answer
            r'(.*?)(?=Q\)|$)',  # Explanation (until next question or end)
            re.DOTALL
        )

        matches = question_pattern.findall(content)
        for match in matches:
            question, a, b, c, d, answer, correct_option, explanation = match
            self.questions.append({
                "metadata": {
                    "date": date},
                "question": question.strip(),
                "a": a.strip(),
                "b": b.strip(),
                "c": c.strip(),
                "d": d.strip(),
                "answer": answer.strip().lower(),
                "explanation": explanation.strip() if explanation.strip() else None,
                "source": self.source,
            })

    def to_csv(self, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=[
                                    "metadata", "question", "a", "b", "c", "d", "answer", "explanation", "source"])
            writer.writeheader()
            writer.writerows(self.questions)


def main(**kwargs):
    pdf_service = PDFService(kwargs["pdf_path"])
    parser = QuestionParser(pdf_service.extract_text(
        pages=kwargs["pages"]), **kwargs)
    parser.parse()
    parser.to_csv(kwargs["output_path"])
