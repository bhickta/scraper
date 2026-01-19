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
            
            # Extract HSN codes if available
            hsn_codes = self.extract_hsn_codes()
            
            # Format the output record with all available fields
            # Handle Status - if Registration Status is empty, mark as "Active" (default assumption)
            status = data.get("Registration Status", "N/A")
            if status == "N/A" or status.strip() == "":
                status = "Active"  # Default assumption if not explicitly stated
            
            record = {
                "Legal Name": data.get("Legal Name", "N/A"),
                "Trade Name": data.get("Trade Name", "N/A"),
                "Status": status,
                "Registration Date": data.get("Registration Date", "N/A"),
                "Constitution": data.get("Entity Type", "N/A"),
                "Principal Place": address,
                "City": city,
                "District": district,
                "State": state,
                "Pincode": pincode,
                "E-Invoice Mandatory": data.get("E-Invoice Mandatory?", "N/A"),
                "Aggregate Turnover": data.get("Aggregate Turnover", "N/A"),
                "Central Jurisdiction": data.get("Central Jurisdiction", "N/A"),
                "State Jurisdiction": data.get("State Jurisdiction", "N/A"),
                "HSN Codes": hsn_codes,
            }
            return [record]

        except Exception as e:
            logger.error(f"Error parsing GST details: {e}")
            return []

    def extract_address_parts(self, address):
        """Extract city, district, state, pincode from address."""
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
    
    def extract_hsn_codes(self):
        """Extract HSN codes from the page."""
        try:
            hsn_codes = []
            
            # Method 1: Look for list items (HSN codes are often in <li> tags)
            list_items = self.soup.find_all('li')
            for li in list_items:
                text = li.get_text(strip=True)
                # HSN codes are 6-8 digit numbers
                if text.isdigit() and len(text) in [6, 8]:
                    hsn_codes.append(text)
            
            # Method 2: Regex search in page text as fallback
            if not hsn_codes:
                code_pattern = re.compile(r'\b(\d{6}|\d{8})\b')
                page_text = self.soup.get_text()
                matches = code_pattern.findall(page_text)
                
                for match in matches:
                    # Exclude dates (years starting with 19/20) and registration numbers
                    if not match.startswith(('19', '20', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12')):
                        hsn_codes.append(match)
            
            # Remove duplicates and return
            unique_codes = list(dict.fromkeys(hsn_codes))
            return ", ".join(unique_codes[:10]) if unique_codes else "N/A"
            
        except Exception as e:
            logger.error(f"Error extracting HSN codes: {e}")
            return "N/A"
