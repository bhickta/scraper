from core import re, json
from core.services.pdf_service import PDFService


class MCQExtractor:
    def __init__(self, pdf_service: PDFService, **kwargs):
        self.pdf_service = pdf_service
        self.mcqs = []
        self.output_path = kwargs.get("output_path", "mcqs.json")

    def process_mcqs(self, text):
        pass

    def to_json(self):
        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(self.mcqs, f, indent=4, ensure_ascii=False)

    def run(self, pages=None):
        self.text = self.pdf_service.extract_text(pages)
        for page_num, page_text in self.text.items():
            self.process_mcqs(page_text)
        self.validate()
        self.to_json()
        return self.mcqs

    def validate(self):
        pass
