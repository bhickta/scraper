from core.services.pdf_service import PDFService
import re
from core import csv


class QuestionParser:
    def __init__(self, text):
        self.text = text
        self.unit = ""
        self.sub_unit = ""
        self.questions = []

    def parse(self):
        self.parse_questions(self.text)

    def parse_questions(self, text):
        units = self.split_units(text)
        for unit_number, unit_title, unit_content in units:
            # print(unit_content, "\n", "-"*20, "\n")
            subunits = self.split_subunits(unit_content)
            for sub_unit_number, sub_unit_title, questions_text in subunits:
                # print(questions_text, "\n", "-"*20, "\n")
                self.extract_questions(
                    unit_title, sub_unit_title, questions_text)

    def split_units(self, text):
        unit_matches = re.split(r"\nUNIT-(\d+) (.+)\n", text)[1:]
        return [(unit_matches[i], unit_matches[i+1].strip(), unit_matches[i+2]) for i in range(0, len(unit_matches), 3)]

    def split_subunits(self, unit_content):
        subunit_matches = list(re.finditer(r"(\d+\.\d+) (.+)", unit_content))

        subunits = []
        for i, match in enumerate(subunit_matches):
            subunit_number = match.group(1)
            subunit_title = match.group(2).strip()

            start_index = match.end()
            end_index = subunit_matches[i + 1].start() if i + \
                1 < len(subunit_matches) else len(unit_content)

            subunit_text = unit_content[start_index:end_index].strip()
            subunits.append((subunit_number, subunit_title, subunit_text))

        return subunits

    def extract_questions(self, unit_title, sub_unit_title, questions_text):
        print(questions_text, "\n", "-"*20, "\n")

        pattern = re.compile(
            r"^\s*(\d+)\.\s*([^\n]+?)(?:\((\d+)M(?:,\s*(\d+)W)?(?:,\s*(CSE|IFoS)\s*(\d+))?\))?",
            re.MULTILINE
        )

        questions = []
        last_match_end = 0

        for match in pattern.finditer(questions_text):
            q_no = int(match.group(1))
            q_text = match.group(2).strip() if match.group(2) else None
            marks = int(match.group(3)) if match.group(3) else None
            words = int(match.group(4)) if match.group(4) else None
            exam = match.group(5) if match.group(5) else None
            year = int(match.group(6)) if match.group(6) else None

            # Find the full question text
            start_idx = match.end()
            next_match = next(pattern.finditer(
                questions_text, start_idx), None)
            end_idx = next_match.start() if next_match else len(questions_text)

            full_question_text = questions_text[start_idx:end_idx].strip()

            # Merge first line with full question text (ensuring no extra spaces)
            full_question = "".join([q_text, full_question_text]).strip()

            questions.append({
                "unit": unit_title,
                "sub_unit": sub_unit_title,
                "question_no": q_no,
                "question": full_question,
                "marks": marks,
                "words": words,
                "exam": exam,
                "year": year
            })

        self.questions.extend(questions)

    def to_csv(self, filename):
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=[
                                    "unit", "sub_unit", "question_no", "question", "marks", "words", "exam", "year"])
            writer.writeheader()
            writer.writerows(self.questions)


def main(**kwargs):
    pdf_service = PDFService(kwargs["pdf_path"])
    parser = QuestionParser(pdf_service.extract_text(pages=kwargs["pages"]))
    parser.parse()
    parser.to_csv(kwargs["output_path"])
