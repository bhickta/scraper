from core.services.pdf_service import PDFService
from core.services.mcq_service import MCQExtractor
import re
from core import csv


def subjects():
    with open("./data/arihant/arihantMCQIndex.csv") as f:
        reader = csv.reader(f)
        next(reader, None)
        return [(row[0] + " - " + row[1], range(int(row[2]) - 1, int(row[3]))) for row in reader]


class ArihantMCQExtractor(MCQExtractor):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.questions = {}
        self.explanations = {}
        self.mcqs = []
        self.source = kwargs.get("source", "Arihant 14000 MCQs")
        self.subject = kwargs.get("subject")

    def process_questions(self):
        mcq_pattern = re.compile(
            r"""^(\d+)\.\s*(.*?)\s*\(a\)\s*(.*?)\s*\(b\)\s*(.*?)\s*\(c\)\s*(.*?)\s*\(d\)\s*(.*?)$""",
            re.DOTALL | re.MULTILINE
        )
        mcqs = mcq_pattern.findall(self.text)

        for mcq in mcqs:
            question_no = int(mcq[0])
            question_text = mcq[1].strip()
            options = {
                "a": mcq[2].strip(),
                "b": mcq[3].strip(),
                "c": mcq[4].strip(),
                "d": mcq[5].strip()
            }

            self.questions[question_no] = {
                "question_no": question_no,
                "question": question_text,
                **options
            }

    def process_explanation(self):
        answer_pattern = re.compile(r"^(\d+)\.\s*\((\w)\)", re.MULTILINE)
        matches = answer_pattern.findall(self.text)

        for match in matches:
            question_no = int(match[0])
            answer = match[1].lower()

            if question_no in self.questions:
                self.explanations[question_no] = {"answer": answer}

    def get_mcqs(self):
        self.mcqs = [
            {
                "question_no": question["question_no"],
                "question": question["question"],
                "a": question.get("a", ""),
                "b": question.get("b", ""),
                "c": question.get("c", ""),
                "d": question.get("d", ""),
                "source": self.source,
                "subject": self.subject,
                "answer": self.explanations.get(question_no, {}).get('answer', "")
            }
            for question_no, question in self.questions.items()
        ]

    def run(self, **kwargs):
        self.text = self.pdf_service.extract_text(**kwargs)
        self.process_questions()
        self.process_explanation()
        self.get_mcqs()
        return self.mcqs


def main(**kwargs):
    pdf_service = PDFService(kwargs["pdf_path"])
    extractor = ArihantMCQExtractor(
        pdf_service=pdf_service, output_path=kwargs["output_path"], subject=kwargs["subject"])
    mcqs = extractor.run(pages=kwargs["pages"])
    extractor.to_csv(
        mode="a", output_path="./data/arihant/output/mcqs.csv")
