import json
import csv
import logging
import re
import os
from utils.scraper import Scraper

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


class GSTScraper(Scraper):
    def parse_page(self):
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
            logging.error(f"Error parsing GST details: {e}")
        return data


def extract_address_parts(address):
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


def load_existing_gstins(output_csv):
    """Load already processed GSTINs from the output CSV to avoid re-scraping."""
    existing_gstins = set()
    if os.path.exists(output_csv):
        with open(output_csv, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            existing_gstins = {row["Gstin"] for row in reader}
    return existing_gstins


def process_csv(input_csv, output_csv):
    existing_gstins = load_existing_gstins(output_csv)
    file_exists = os.path.exists(output_csv)

    with open(input_csv, 'r', encoding='utf-8') as infile, open(output_csv, 'a', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = ["Gstin", "Legal Name", "Trade Name", "Status",
                      "Constitution", "Principal Place", "City", "District", "State", "Pincode"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)

        # ✅ Ensures headers if file is empty
        if not file_exists or os.stat(output_csv).st_size == 0:
            writer.writeheader()

        for row in reader:
            gstin = row.get("gstin", "").strip()
            if not gstin:
                logging.warning("Skipping row with missing GSTIN")
                continue
            if gstin in existing_gstins:
                logging.info(f"Skipping already processed GSTIN: {gstin}")
                continue

            url = f"https://gst.jamku.app/gstin/{gstin}"
            scraper = GSTScraper(base_url=url)
            data = scraper.scrape()

            address = data.get("Place Of Business (Address)", "N/A")
            city, district, state, pincode = extract_address_parts(address)

            writer.writerow({
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
            })
            logging.info(f"Processed GSTIN: {gstin}")
            existing_gstins.add(gstin)


def main(input_csv="input.csv", output_csv="output.csv"):
    process_csv(input_csv, output_csv)
    logging.info(f"Scraping completed. Results saved to {output_csv}")
