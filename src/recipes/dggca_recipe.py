import re
import unicodedata
from typing import Any, Dict, List
from src.core.base_pdf import BasePDFExtractor

class DggcaExtractor(BasePDFExtractor):
    """
    Extractor for DGGCA PDF documents.
    """

    def parse(self, text: str) -> List[Dict[str, Any]]:
        normalized_text = self.normalize_text(text)
        dates = self.split_dates(normalized_text)
        all_questions = []
        for date, content in dates:
            questions = self.extract_questions(date, content)
            all_questions.extend(questions)
        return all_questions

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
        questions = []
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
            questions.append({
                "metadata": {
                    "date": date},
                "question": question.strip(),
                "a": a.strip(),
                "b": b.strip(),
                "c": c.strip(),
                "d": d.strip(),
                "answer": answer.strip().lower(),
                "explanation": explanation.strip() if explanation.strip() else None,
                "source": self.kwargs.get("source"),
            })
        return questions
