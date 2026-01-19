import logging
import re
import csv
import os
from typing import Any, Dict, List, Optional
from src.core.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class GstExtractor(BaseScraper):
    """
    Extractor for GST details.
    """
    def parse_page(self) -> List[Dict[str, Any]]:
        # The BaseScraper extract/scrape method calls this.
        # It expects a list of dicts.
        data = {}
        try:
            gstin_details = self.soup.find_all("p", class_="text-cyan-700")

            for p in gstin_details:
                label = p.get_text(strip=True).title()
                value_element = p.find_next_sibling(["h2", "p"])
                value = value_element.get_text(
                    strip=True).title() if value_element else "N/A"
                data[label] = value

            # Address processing
            address = data.get("Place Of Business (Address)", "N/A")
            city, district, state, pincode = self.extract_address_parts(address)
            
            # Format the output record
            record = {
                "Legal Name": data.get("Legal Name", "N/A"),
                "Trade Name": data.get("Trade Name", "N/A"),
                "Status": data.get("Registration Status", "N/A"),
                "Constitution": data.get("Entity Type", "N/A"),
                "Principal Place": address,
                "City": city,
                "District": district,
                "State": state,
                "Pincode": pincode
            }
            return [record]

        except Exception as e:
            logger.error(f"Error parsing GST details: {e}")
            return []

    def extract_address_parts(self, address):
        parts = address.split(", ")

        if len(parts) >= 4:
            city = parts[-4].strip()
            district = parts[-3].strip()
            state = parts[-2].strip()
            pincode_match = re.search(r"\d{6}$", parts[-1])
            pincode = pincode_match.group() if pincode_match else "N/A"
            return city, district, state, pincode
        else:
            return "N/A", "N/A", "N/A", "N/A"

