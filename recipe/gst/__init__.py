import json
import csv
import logging
from utils.scraper import Scraper

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


class GSTScraper(Scraper):
    def parse_page(self):
        data = {}
        try:
            gstin_details = self.soup.find_all("p", class_="text-cyan-700")

            for p in gstin_details:
                label = p.get_text(strip=True)
                value_element = p.find_next_sibling(["h2", "p"])
                value = value_element.get_text(
                    strip=True) if value_element else "N/A"
                data[label] = value

        except Exception as e:
            logging.error(f"Error parsing GST details: {e}")
        return data


def process_csv(input_csv, output_csv):
    with open(input_csv, 'r', encoding='utf-8') as infile, open(output_csv, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = ["GSTIN", "Legal Name", "Trade Name",
                      "Status", "Constitution", "Principal Place"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            gstin = row.get("gstin", "").strip()
            if not gstin:
                logging.warning("Skipping row with missing GSTIN")
                continue

            url = f"https://gst.jamku.app/gstin/{gstin}"
            scraper = GSTScraper(base_url=url)
            data = scraper.scrape()

            writer.writerow({
                "GSTIN": gstin,
                "Legal Name": data.get("Legal Name", "N/A"),
                "Trade Name": data.get("Trade Name", "N/A"),
                "Status": data.get("Registration Status", "N/A"),  # Fixed key
                "Constitution": data.get("Entity Type", "N/A"),  # Fixed key
                # Fixed key
                "Principal Place": data.get("Place of Business (Address)", "N/A")
            })
            logging.info(f"Processed GSTIN: {gstin}")


def main(input_csv="input.csv", output_csv="output.csv"):
    process_csv(input_csv, output_csv)
    logging.info(f"Scraping completed. Results saved to {output_csv}")


if __name__ == "__main__":
    main()
