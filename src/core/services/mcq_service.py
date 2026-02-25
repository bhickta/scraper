"""
MCQExtractor — base class for MCQ extraction from PDF documents.
"""

import csv
import json
import logging

from src.core.services.pdf_service import PDFService

logger = logging.getLogger(__name__)


class MCQExtractor:
    """Base MCQ extractor. Subclasses implement ``process_mcqs()``."""

    def __init__(self, **kwargs):
        self.pdf_service: PDFService = kwargs["pdf_service"]
        self.output_path: str = kwargs.get("output_path", "mcqs.json")
        self.mcqs: list = []
        self.text: str = ""

    def process_mcqs(self) -> None:
        """Override in subclass to implement MCQ parsing logic."""
        pass

    def to_json(self, output_path: str | None = None, mode: str = "w") -> None:
        path = output_path or self.output_path
        with open(path, mode=mode, encoding="utf-8") as f:
            json.dump(self.mcqs, f, indent=4, ensure_ascii=False)

    def to_csv(self, output_path: str | None = None, mode: str = "w") -> None:
        path = output_path or self.output_path
        with open(path, mode=mode, encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "question", "answer", "explanation",
                "a", "b", "c", "d", "e", "f", "source", "subject",
            ])
            for mcq in self.mcqs:
                writer.writerow([
                    mcq["question"],
                    mcq["answer"],
                    mcq.get("explanation", ""),
                    mcq["a"],
                    mcq["b"],
                    mcq["c"],
                    mcq["d"],
                    mcq.get("e", ""),
                    mcq.get("f", ""),
                    mcq["source"],
                    mcq["subject"],
                ])

    def run(self, pages=None):
        self.text = self.pdf_service.extract_text(pages)
        self.process_mcqs()
        self.validate()
        self.to_json()
        return self.mcqs

    def validate(self) -> None:
        """Override in subclass to add validation rules."""
        pass
