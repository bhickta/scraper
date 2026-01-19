from typing import Any, Dict, List, Optional
import os
import csv
import json
import logging
from src.core.interfaces import IDataExtractor
from src.core.services.pdf_service import PDFService

logger = logging.getLogger(__name__)

class BasePDFExtractor(IDataExtractor):
    """
    Base class for PDF extraction.
    """
    def __init__(self, pdf_path: str, **kwargs):
        self.pdf_path = pdf_path
        self.pdf_service = PDFService(pdf_path)
        self.kwargs = kwargs

    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract data from the PDF.
        """
        try:
            pages = kwargs.get('pages', self.kwargs.get('pages'))
            text_content = self.pdf_service.extract_text(pages=pages)
            if not text_content:
                logger.warning(f"No text extracted from PDF: {self.pdf_path}")
                return []
            
            return self.parse(text_content)
        except Exception as e:
            logger.error(f"PDF extraction failed: {e}")
            raise

    def parse(self, text: str) -> List[Dict[str, Any]]:
        """
        Parse the extracted text. Subclasses must implement this.
        """
        raise NotImplementedError("Subclasses must implement parse method")

    def save(self, data: List[Dict[str, Any]], output_path: str):
        """
        Save the extracted data to a file.
        """
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        if output_path.endswith('.csv'):
             self._save_to_csv(data, output_path)
        elif output_path.endswith('.json'):
             self._save_to_json(data, output_path)
        else:
             raise ValueError("Unsupported format. Use .csv or .json")
    
    def _save_to_csv(self, data: List[Dict[str, Any]], output_path: str):
        if not data:
            logger.warning("No data to save")
            return
            
        keys = data[0].keys()
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)
            
    def _save_to_json(self, data: List[Dict[str, Any]], output_path: str):
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
