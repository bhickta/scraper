"""Agriculture recipe — question parser for Krushna-style PYQ PDFs."""

import csv
import re
from typing import Optional

from src.core.services.pdf_service import PDFService


class QuestionParser:
    """Parse structured questions from agriculture PYQ PDF text."""

    def __init__(self, text: str):
        self.text = text
        self.unit = ""
        self.sub_unit = ""
        self.questions = []

    def parse(self) -> None:
        self._parse_questions(self.text)

    def _parse_questions(self, text: str) -> None:
        units = self._split_units(text)
        for unit_number, unit_title, unit_content in units:
            subunits = self._split_subunits(unit_content)
            for sub_unit_number, sub_unit_title, questions_text in subunits:
                self._extract_questions(unit_title, sub_unit_title, questions_text)

    @staticmethod
    def _split_units(text: str) -> list:
        unit_matches = re.split(r"\nUNIT-(\d+) (.+)\n", text)[1:]
        return [
            (unit_matches[i], unit_matches[i + 1].strip(), unit_matches[i + 2])
            for i in range(0, len(unit_matches), 3)
        ]

    @staticmethod
    def _split_subunits(unit_content: str) -> list:
        subunit_matches = list(re.finditer(r"(\d+\.\d+) (.+)", unit_content))
        subunits = []
        for i, match in enumerate(subunit_matches):
            subunit_number = match.group(1)
            subunit_title = match.group(2).strip()
            start_index = match.end()
            end_index = (
                subunit_matches[i + 1].start()
                if i + 1 < len(subunit_matches)
                else len(unit_content)
            )
            subunit_text = unit_content[start_index:end_index].strip()
            subunits.append((subunit_number, subunit_title, subunit_text))
        return subunits

    def _extract_questions(
        self, unit_title: str, sub_unit_title: str, questions_text: str
    ) -> None:
        pattern = re.compile(
            r"^\s*(\d+)\.\s*([^\n]+?)(?:\((\d+)M(?:,\s*(\d+)W)?(?:,\s*(CSE|IFoS)\s*(\d+))?\))?",
            re.MULTILINE,
        )
        questions = []
        for match in pattern.finditer(questions_text):
            q_no = int(match.group(1))
            q_text = match.group(2).strip() if match.group(2) else None
            marks = int(match.group(3)) if match.group(3) else None
            words = int(match.group(4)) if match.group(4) else None
            exam = match.group(5) if match.group(5) else None
            year = int(match.group(6)) if match.group(6) else None

            start_idx = match.end()
            next_match = next(pattern.finditer(questions_text, start_idx), None)
            end_idx = next_match.start() if next_match else len(questions_text)
            full_question_text = questions_text[start_idx:end_idx].strip()
            full_question = "".join([q_text, full_question_text]).strip()

            questions.append({
                "unit": unit_title,
                "sub_unit": sub_unit_title,
                "question_no": q_no,
                "question": full_question,
                "marks": marks,
                "words": words,
                "exam": exam,
                "year": year,
            })

        self.questions.extend(questions)

    def to_csv(self, filename: str) -> None:
        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=[
                    "unit", "sub_unit", "question_no", "question",
                    "marks", "words", "exam", "year",
                ],
            )
            writer.writeheader()
            writer.writerows(self.questions)


def main(**kwargs) -> None:
    pdf_service = PDFService(kwargs["pdf_path"])
    parser = QuestionParser(pdf_service.extract_text(pages=kwargs["pages"]))
    parser.parse()
    parser.to_csv(kwargs["output_path"])
