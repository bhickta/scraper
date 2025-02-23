from core import re, json, csv, os
from core.services.pdf_service import PDFService


class MCQExtractor:
    def __init__(self, **kwargs):
        self.pdf_service = kwargs["pdf_service"]
        self.output_path = kwargs.get("output_path", "mcqs.json")

    def process_mcqs(self):
        pass

    def to_json(self, output_path=None, mode="w"):
        with open(output_path, mode=mode, encoding="utf-8") as f:
            json.dump(self.mcqs, f, indent=4, ensure_ascii=False)

    def to_csv(self, output_path=None, mode="w"):

        with open(output_path, mode=mode, encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(['question', 'answer', 'explanation',
                            'a', 'b', 'c', 'd', 'e', 'f', 'source', 'subject'])

            for mcq in self.mcqs:
                writer.writerow([
                    mcq['question'],
                    mcq['answer'],
                    mcq.get('explanation', ''),
                    mcq['a'],
                    mcq['b'],
                    mcq['c'],
                    mcq['d'],
                    mcq.get('e', ''),
                    mcq.get('f', ''),
                    mcq['source'],
                    mcq["subject"],
                ])

    def run(self, pages=None):
        self.text = self.pdf_service.extract_text(pages)
        self.process_mcqs()
        self.validate()
        self.to_json()
        return self.mcqs

    def validate(self):
        pass
