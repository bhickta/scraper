from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

class IDataExtractor(ABC):
    """
    Interface for data extractors (scrapers, parsers, etc).
    """

    @abstractmethod
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract data from the source.
        
        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing the extracted data.
        """
        pass

    @abstractmethod
    def save(self, data: List[Dict[str, Any]], output_path: str):
        """
        Save the extracted data to a file.
        """
        pass
