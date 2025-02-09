from core.services.pdf_service import PDFService
from core.services.mcq_service import MCQExtractor
from core import re


class VisionMCQExtractor(MCQExtractor):

    def process_mcqs(self):
        mcq_pattern = re.compile(
            r"^\s*(\d+)\.\s*\n(.*?)(?=\n\s*\d+\.\s*\n|\Z)",
            re.DOTALL | re.MULTILINE
        )
        last_question_no = 0
        for mcq in mcq_pattern.finditer(self.text):
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

    def process_explanation(self):
        explanation_pattern = re.compile(
            r"^Q\s*(\d+)\.", re.DOTALL | re.MULTILINE
        )
        last_question_no = 0
        content_list = []

        for match in explanation_pattern.finditer(self.text):
            question_no = int(match.group(1))
            if question_no != last_question_no + 1 and last_question_no != 0:
                continue

            start = match.end()
            next_match = next(explanation_pattern.finditer(
                self.text[start:]), None)
            end = start + (next_match.start()
                           if next_match else len(self.text))

            content = self.text[start:end].strip()
            content_list.append(content)
            last_question_no = question_no

        print(len(content_list))

    def run(self, **kwargs):
        super().run(**kwargs)
        self.process_explanation()


if __name__ == "__main__":
    pdf_service = PDFService(
        "./data/VISION IAS PRELIMS-2024 _TEST- 01-32.PDF")
    extractor = VisionMCQExtractor(pdf_service, output_path="vision_mcqs.json")
    mcqs = extractor.run(pages=range(20, 59))
