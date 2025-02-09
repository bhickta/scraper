from core import re, json
from core.services.pdf_service import PDFService


class MCQExtractor:
    def __init__(self, **kwargs):
        self.pdf_service = kwargs["pdf_service"]
        self.output_path = kwargs.get("output_path", "mcqs.json")

    def process_mcqs(self):
        pass

    def to_json(self):
        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(self.mcqs, f, indent=4, ensure_ascii=False)

    def run(self, pages=None):
        self.text = self.pdf_service.extract_text(pages)
        self.process_mcqs()
        self.validate()
        self.to_json()
        return self.mcqs

    def validate(self):
        pass
