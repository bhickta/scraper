"""
BasePDFExtractor — base class for PDF data extraction.

Provides the common extract → parse pipeline for PDF documents.
"""

import logging
from typing import Any, Dict, List

from src.core.data_saver import DataSaverMixin
from src.core.interfaces import IDataExtractor
from src.core.services.pdf_service import PDFService

logger = logging.getLogger(__name__)


class BasePDFExtractor(DataSaverMixin, IDataExtractor):
    """
    Base class for PDF extraction.

    Subclasses must implement ``parse()`` to process the extracted text.
    """

    def __init__(self, pdf_path: str, **kwargs):
        self.pdf_path = pdf_path
        self.pdf_service = PDFService(pdf_path)
        self.kwargs = kwargs

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """Extract data from the PDF."""
        try:
            pages = kwargs.get("pages", self.kwargs.get("pages"))
            text_content = self.pdf_service.extract_text(pages=pages)
            if not text_content:
                logger.warning(f"No text extracted from PDF: {self.pdf_path}")
                return []

            return self.parse(text_content)
        except Exception as e:
            logger.error(f"PDF extraction failed: {e}")
            raise

    def parse(self, text: str) -> List[Dict[str, Any]]:
        """Parse the extracted text. Subclasses must implement this."""
        raise NotImplementedError("Subclasses must implement parse method")
