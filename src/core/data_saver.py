"""
DataSaverMixin — shared save logic for all extractors.

Extracted from BaseScraper and BasePDFExtractor to eliminate duplication.
"""

import csv
import json
import logging
import os
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class DataSaverMixin:
    """Mixin providing save-to-file capability for data extractors."""

    def save(self, data: List[Dict[str, Any]], output_path: str) -> None:
        """
        Save extracted data to CSV or JSON.

        Args:
            data: List of dictionaries to save.
            output_path: Destination file path (.csv or .json).

        Raises:
            ValueError: If the output format is unsupported.
        """
        dir_name = os.path.dirname(output_path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

        if output_path.endswith(".csv"):
            self._save_to_csv(data, output_path)
        elif output_path.endswith(".json"):
            self._save_to_json(data, output_path)
        else:
            raise ValueError("Unsupported format. Use .csv or .json")

    @staticmethod
    def _save_to_csv(data: List[Dict[str, Any]], output_path: str) -> None:
        if not data:
            logger.warning("No data to save")
            return

        keys = data[0].keys()
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)

    @staticmethod
    def _save_to_json(data: List[Dict[str, Any]], output_path: str) -> None:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
