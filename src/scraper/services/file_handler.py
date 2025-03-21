import os
import csv
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from scraper.config.settings import OUTPUT_DIR

logger = logging.getLogger(__name__)


class FileHandler:
    """
    Handles file operations for scraped data.
    """

    @staticmethod
    def ensure_directory(directory_path):
        """
        Ensure the directory exists, create if it doesn't.

        Args:
            directory_path (str or Path): Directory path to ensure

        Returns:
            Path: Path object of the directory
        """
        path = Path(directory_path)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def save_csv(data: List[Dict[str, Any]],
                 file_path: Union[str, Path],
                 fieldnames: Optional[List[str]] = None,
                 append: bool = False) -> str:
        """
        Save data to a CSV file.

        Args:
            data (list): List of dictionaries to save
            file_path (str or Path): Path to save the CSV file
            fieldnames (list, optional): List of field names. Defaults to keys of first item.
            append (bool, optional): Whether to append to existing file. Defaults to False.

        Returns:
            str: Path to the saved file
        """
        file_path = Path(file_path)

        # Ensure directory exists
        FileHandler.ensure_directory(file_path.parent)

        # Determine if file exists
        file_exists = file_path.exists()

        # Get fieldnames if not provided
        if fieldnames is None and data:
            fieldnames = list(data[0].keys())

        # Safety check to ensure fieldnames is not None
        if fieldnames is None:
            fieldnames = []

        # Determine mode
        mode = 'a' if append and file_exists else 'w'

        try:
            with open(file_path, mode, newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)

                # Write header if file is new or not appending
                if not (append and file_exists):
                    writer.writeheader()

                # Write data rows
                writer.writerows(data)

            logger.info(f"Successfully saved data to {file_path}")
            return str(file_path)

        except Exception as e:
            logger.error(f"Error saving CSV file {file_path}: {e}")
            raise

    @staticmethod
    def read_csv(file_path: Union[str, Path]) -> List[Dict[str, str]]:
        """
        Read data from a CSV file.

        Args:
            file_path (str or Path): Path to the CSV file

        Returns:
            list: List of dictionaries representing the CSV data
        """
        file_path = Path(file_path)

        if not file_path.exists():
            logger.error(f"CSV file does not exist: {file_path}")
            return []

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                return list(reader)

        except Exception as e:
            logger.error(f"Error reading CSV file {file_path}: {e}")
            return []

    @staticmethod
    def save_excel(data: List[Dict[str, Any]],
                   file_path: Union[str, Path],
                   sheet_name: str = "Sheet1") -> str:
        """
        Save data to an Excel file.

        Args:
            data (list): List of dictionaries to save
            file_path (str or Path): Path to save the Excel file
            sheet_name (str, optional): Name of the sheet. Defaults to "Sheet1".

        Returns:
            str: Path to the saved file
        """
        file_path = Path(file_path)

        # Ensure directory exists
        FileHandler.ensure_directory(file_path.parent)

        try:
            # Convert data to pandas DataFrame
            df = pd.DataFrame(data)

            # Save to Excel
            df.to_excel(file_path, sheet_name=sheet_name, index=False)

            logger.info(f"Successfully saved data to Excel file {file_path}")
            return str(file_path)

        except Exception as e:
            logger.error(f"Error saving Excel file {file_path}: {e}")
            raise

    @staticmethod
    def read_excel(file_path: Union[str, Path],
                   sheet_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Read data from an Excel file.

        Args:
            file_path (str or Path): Path to the Excel file
            sheet_name (str, optional): Name of the sheet to read. Defaults to first sheet.

        Returns:
            list: List of dictionaries representing the Excel data
        """
        file_path = Path(file_path)

        if not file_path.exists():
            logger.error(f"Excel file does not exist: {file_path}")
            return []

        try:
            # If sheet_name is None, pandas will read the first sheet
            if sheet_name is None:
                # Read the first sheet only
                df = pd.read_excel(file_path, sheet_name=0)
                records = df.to_dict('records')
                logger.info(
                    f"Successfully read {len(records)} records from first sheet of Excel file {file_path}")
                return records
            else:
                # Read the specific sheet
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                records = df.to_dict('records')
                logger.info(
                    f"Successfully read {len(records)} records from sheet '{sheet_name}' of Excel file {file_path}")
                return records

        except Exception as e:
            logger.error(f"Error reading Excel file {file_path}: {e}")
            return []

    @staticmethod
    def save_json(data: Union[Dict[str, Any], List[Any]],
                  file_path: Union[str, Path]) -> str:
        """
        Save data to a JSON file.

        Args:
            data (dict or list): Data to save
            file_path (str or Path): Path to save the JSON file

        Returns:
            str: Path to the saved file
        """
        file_path = Path(file_path)

        # Ensure directory exists
        FileHandler.ensure_directory(file_path.parent)

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            logger.info(f"Successfully saved JSON data to {file_path}")
            return str(file_path)

        except Exception as e:
            logger.error(f"Error saving JSON file {file_path}: {e}")
            raise

    @staticmethod
    def read_json(file_path: Union[str, Path]) -> Union[Dict[str, Any], List[Any]]:
        """
        Read data from a JSON file.

        Args:
            file_path (str or Path): Path to the JSON file

        Returns:
            dict or list: Data from the JSON file
        """
        file_path = Path(file_path)

        if not file_path.exists():
            logger.error(f"JSON file does not exist: {file_path}")
            return {}

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)

        except Exception as e:
            logger.error(f"Error reading JSON file {file_path}: {e}")
            return {}

    @staticmethod
    def read_data(file_path: Union[str, Path]) -> List[Dict[str, Any]]:
        """
        Read data from a file based on its extension.

        Args:
            file_path (str or Path): Path to the file

        Returns:
            list: List of dictionaries representing the data
        """
        file_path = Path(file_path)

        if not file_path.exists():
            logger.error(f"File does not exist: {file_path}")
            return []

        # Determine file type by extension
        extension = file_path.suffix.lower()

        if extension == '.csv':
            return FileHandler.read_csv(file_path)
        elif extension in ['.xlsx', '.xls']:
            return FileHandler.read_excel(file_path)
        elif extension == '.json':
            data = FileHandler.read_json(file_path)
            # Ensure we return a list of dictionaries
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            else:
                logger.error(
                    f"Unexpected data format in JSON file: {file_path}")
                return []
        else:
            logger.error(f"Unsupported file extension: {extension}")
            return []
