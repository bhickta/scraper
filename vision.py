from core.services.pdf_service import PDFService
from core.services.mcq_service import MCQExtractor
from core import re


class VisionMCQExtractor(MCQExtractor):
    def process_mcqs(self, text):
        mcq_pattern = re.compile(
            r"^\s*(\d+)\.\s*\n(.*?)(?=\n\s*\d+\.\s*\n|\Z)",
            re.DOTALL | re.MULTILINE
        )
        questions = mcq_pattern.findall(text)
        self.process_questions(questions)

    def process_questions(self, questions):
        for question_no, question in questions:
            print(question)
            question, options = self.extract_mcq(question)
            self.mcqs.append({
                "question_no": question_no,
                "question": question,
                "a": options.get("a", ""),
                "b": options.get("b", ""),
                "c": options.get("c", ""),
                "d": options.get("d", ""),
                "e": options.get("e", ""),
                "f": options.get("f", ""),
            })

    def extract_mcq(self, text):
        match = re.search(r"^(.*?)(?=\n\s*\(\w\))", text, re.DOTALL)
        question = match.group(1).strip() if match else None

        options = {key: value.strip()
                   for key, value in re.findall(r"\((\w)\)\s*([^\(\)]*)", text)}

        return question, options


if __name__ == "__main__":
    pdf_service = PDFService(
        "./data/VISION IAS PRELIMS-2024 _TEST- 01-32.PDF")
    extractor = VisionMCQExtractor(pdf_service, output_path="vision_mcqs.json")
    mcqs = extractor.run(pages=range(5, 6))
