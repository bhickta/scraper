import re
import logging
import os
from pathlib import Path
from typing import Dict, Tuple, Set, List, Any, Optional

from scraper.core.base import BaseScraper
from scraper.services.file_handler import FileHandler

logger = logging.getLogger(__name__)


class GSTScraper(BaseScraper):
    """
    GST information scraper that parses details from GST portal.
    """

    def parse_page(self) -> Dict[str, str]:
        """
        Parse the GST details from the page.

        Returns:
            Dict[str, str]: Dictionary of GST information
        """
        data = {}
        try:
            gstin_details = self.soup.find_all("p", class_="text-cyan-700")

            for p in gstin_details:
                label = p.get_text(strip=True).title()
                value_element = p.find_next_sibling(["h2", "p"])
                value = value_element.get_text(
                    strip=True).title() if value_element else "N/A"
                data[label] = value

        except Exception as e:
            logger.error(f"Error parsing GST details: {e}")

        return data


def extract_address_parts(address: str) -> Tuple[str, str, str, str]:
    """
    Extract city, district, state, and pincode from an address string.

    Args:
        address (str): Full address string

    Returns:
        Tuple[str, str, str, str]: Tuple of (city, district, state, pincode)
    """
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


def load_existing_gstins(output_file: str) -> Set[str]:
    """
    Load already processed GSTINs from the output file to avoid re-scraping.

    Args:
        output_file (str): Path to the output file (CSV or Excel)

    Returns:
        Set[str]: Set of already processed GSTINs
    """
    existing_gstins = set()
    if os.path.exists(output_file):
        data = FileHandler.read_data(output_file)
        existing_gstins = {row.get("Gstin", "") for row in data}
    return existing_gstins


def save_result(result: Dict[str, Any], output_file: str, is_first: bool = False) -> None:
    """
    Save a single result to the output file.

    Args:
        result (Dict[str, Any]): The result data to save
        output_file (str): Path to the output file
        is_first (bool): Whether this is the first result being saved
    """
    output_path = Path(output_file)
    extension = output_path.suffix.lower()

    # For the first result, we may need to create the file
    append_mode = not is_first and os.path.exists(output_file)

    if extension == '.csv':
        # For CSV, we can append to the file
        fieldnames = list(result.keys())
        FileHandler.save_csv([result], output_file,
                             fieldnames=fieldnames, append=append_mode)
    elif extension in ['.xlsx', '.xls']:
        # For Excel, we need to read the existing data, append the new row, and write it all back
        if append_mode:
            # Read existing data
            existing_data = FileHandler.read_excel(output_file)
            # Append new data
            existing_data.append(result)
            # Write back to file
            FileHandler.save_excel(
                existing_data, output_file, sheet_name="GST Data")
        else:
            # Create new file with just this result
            FileHandler.save_excel(
                [result], output_file, sheet_name="GST Data")
    else:
        # Default to CSV
        logger.warning(
            f"Unrecognized output file extension: {extension}. Defaulting to CSV.")
        csv_path = output_path.with_suffix('.csv')
        fieldnames = list(result.keys())
        FileHandler.save_csv([result], csv_path,
                             fieldnames=fieldnames, append=append_mode)


def process_data(input_file: str, output_file: str) -> None:
    """
    Process a file containing GSTINs and save the results.

    Args:
        input_file (str): Path to the input file containing GSTINs (CSV or Excel)
        output_file (str): Path to save the output file (CSV or Excel)
    """
    existing_gstins = load_existing_gstins(output_file)
    file_exists = os.path.exists(output_file)

    # Read input data using the appropriate method based on file extension
    input_data = FileHandler.read_data(input_file)

    # Track whether we've processed at least one GSTIN
    is_first_result = not file_exists
    processed_count = 0

    # Define fieldnames
    fieldnames = ["Gstin", "Legal Name", "Trade Name", "Status",
                  "Constitution", "Principal Place", "City", "District", "State", "Pincode"]

    for row in input_data:
        # Try different column names that might be used for GSTIN
        gstin = None
        for key in ["gstin", "Gstin", "GSTIN", "gst_number", "GST Number"]:
            if key in row and row[key]:
                gstin = row[key].strip()
                break

        if not gstin:
            logger.warning("Skipping row with missing GSTIN")
            continue

        if gstin in existing_gstins:
            logger.info(f"Skipping already processed GSTIN: {gstin}")
            continue

        try:
            url = f"https://gst.jamku.app/gstin/{gstin}"
            scraper = GSTScraper(base_url=url)
            data = scraper.scrape()

            address = data.get("Place Of Business (Address)", "N/A")
            city, district, state, pincode = extract_address_parts(address)

            result = {
                "Gstin": gstin,
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

            # Save the result immediately
            save_result(result, output_file, is_first=is_first_result)

            # Update tracking variables
            is_first_result = False
            processed_count += 1
            existing_gstins.add(gstin)

            logger.info(f"Processed and saved GSTIN: {gstin}")

        except Exception as e:
            logger.error(f"Error processing GSTIN {gstin}: {e}")
            # Continue processing other GSTINs even if one fails

    if processed_count == 0:
        logger.warning("No new GSTINs were processed.")
    else:
        logger.info(f"Completed processing {processed_count} new GSTINs.")


def main(input_file: str = "input.csv", output_file: str = "output.csv") -> None:
    """
    Main entry point for the GST scraper.

    Args:
        input_file (str, optional): Path to the input file (CSV or Excel). Defaults to "input.csv".
        output_file (str, optional): Path to the output file (CSV or Excel). Defaults to "output.csv".
    """
    process_data(input_file, output_file)
    logger.info(f"GST scraping completed. Results saved to {output_file}")
