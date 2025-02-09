from core.services.pdf_service import PDFService
from core.services.mcq_service import MCQExtractor
from core import re


class VisionMCQExtractor(MCQExtractor):

    def process_mcqs(self, text):
        mcq_pattern = re.compile(
            r"^\s*(\d+)\.\s*\n(.*?)(?=\n\s*\d+\.\s*\n|\Z)",
            re.DOTALL | re.MULTILINE
        )
        last_question_no = 0
        for mcq in mcq_pattern.finditer(text):
            question_no = int(mcq.group(1))
            if question_no != last_question_no + 1 and last_question_no != 0:

                continue

            question_text = mcq.group(2).strip()
            options_pattern = re.compile(r"^[a-d]\)\s*(.+)$", re.MULTILINE)
            options = options_pattern.findall(question_text)
            question_text = options_pattern.sub("", question_text).strip()
            self.mcqs.append({
                "question": question_text,
                "options": options,
                "number": question_no
            })
            last_question_no = question_no

    def run(self, **kwargs):
        super().run(**kwargs)
        print(len(self.mcqs))

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
    mcqs = extractor.run(pages=range(60, 79))
