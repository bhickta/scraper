"""
PDFService — text extraction from PDF documents using PyMuPDF (fitz).
"""

import logging

import fitz

logger = logging.getLogger(__name__)


class PDFService:
    """Extracts text from PDF documents."""

    def __init__(self, pdf_path: str, **kwargs):
        self.pdf_path = pdf_path

    def extract_text_dict(self, pages=None) -> dict | None:
        """Extract text as a dict mapping page number → text."""
        extracted_text = {}
        try:
            doc = fitz.open(self.pdf_path)
            target_pages = range(doc.page_count) if pages is None else pages
            for page_num in target_pages:
                page = doc[page_num]
                text = page.get_text("text")
                extracted_text[page_num] = text
            doc.close()
        except Exception as e:
            logger.error(f"Error processing PDF: {e}")
            return None
        return extracted_text

    def extract_text_string(self, pages=None) -> str | None:
        """Extract text as a single concatenated string."""
        extracted_text = ""
        try:
            doc = fitz.open(self.pdf_path)
            target_pages = range(doc.page_count) if pages is None else pages
            for page_num in target_pages:
                page = doc[page_num]
                text = page.get_text("text")
                extracted_text += text
            doc.close()
        except Exception as e:
            logger.error(f"Error processing PDF: {e}")
            return None
        return extracted_text

    def extract_text(self, pages=None) -> str | None:
        """Extract text (returns a string by default)."""
        return self.extract_text_string(pages)
