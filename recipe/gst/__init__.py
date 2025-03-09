import json
import logging
import csv
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("selenium.webdriver.remote.remote_connection").setLevel(
    logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)


class GSTScraper:
    def __init__(self, remote=False, hub_url='http://selenium-hub:4444/wd/hub'):
        self.remote = remote
        self.hub_url = hub_url
        self.driver = self._init_driver()

    def _init_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")
        chrome_options.add_argument("--disable-features=NetworkService")
        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-sync")
        chrome_options.add_argument("--disable-translate")
        chrome_options.add_argument("--disable-default-apps")
        chrome_options.add_argument("--disable-client-side-phishing-detection")
        chrome_options.add_argument("--disable-hang-monitor")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        chrome_options.add_argument("--disable-renderer-backgrounding")

        return webdriver.Remote(command_executor=self.hub_url, options=chrome_options) if self.remote else webdriver.Chrome(options=chrome_options)

    def parse_page(self, gstin):
        url = f"https://app.signalx.ai/gstin-verification/{gstin}"
        attempts = 3
        for attempt in range(attempts):
            try:
                self.driver.get(url)
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                business_details = self._extract_business_details()
                return {
                    "gstin": gstin,
                    "Legal Name": business_details.get("Legal Name of Business", ""),
                    "Trade Name": business_details.get("Trade Name", ""),
                    "Effective Date": business_details.get("Effective Date of registration", ""),
                    "Constitution": business_details.get("Constitution of Business", ""),
                    "GSTIN Status": business_details.get("GSTIN / UIN Status", ""),
                    "Taxpayer Type": business_details.get("Taxpayer Type", ""),
                    "Principal Place": business_details.get("Principal Place of Business", ""),
                    "goods_services": self._extract_goods_services(),
                    "filing_history": self._extract_filing_history(),
                }
            except Exception as e:
                logging.warning(
                    f"Retrying ({attempt + 1}/{attempts}) for {gstin} due to {e}")
                time.sleep(2)

        logging.error(f"Failed to fetch data for {gstin}")
        return None

    def _extract_business_details(self):
        details = {}
        try:
            elements = WebDriverWait(self.driver, 5).until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR,
                     "div.MuiGrid-item[style*='margin-bottom: 1rem;']")
                )
            )
            for element in elements:
                h6_element = element.find_element(By.TAG_NAME, "h6")
                p_element = element.find_element(By.TAG_NAME, "p")
                details[h6_element.text] = p_element.text
        except Exception:
            pass
        return details

    def _extract_goods_services(self):
        goods_services = []
        try:
            rows = self.driver.find_elements(
                By.XPATH, "//table[contains(@class, 'MuiTable-root')]//tbody//tr")
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) == 2:
                    goods_services.append(
                        {"HSN": cells[0].text, "Description": cells[1].text})
        except Exception:
            pass
        return goods_services

    def _extract_filing_history(self):
        filing_history = {}
        try:
            history_divs = self.driver.find_elements(
                By.XPATH, "//div[contains(@class, 'MuiGrid-item') and contains(@class, 'MuiGrid-grid-md-6')]"
            )
            for div in history_divs:
                try:
                    title = div.find_element(By.TAG_NAME, "h6").text
                    rows = div.find_elements(By.XPATH, ".//table//tbody//tr")
                    table_data = [
                        {"Financial Year": cells[0].text, "Tax Period": cells[1].text,
                         "Date of filing": cells[2].text, "Status": cells[3].text}
                        for row in rows if (cells := row.find_elements(By.TAG_NAME, "td"))
                    ]
                    filing_history[title] = table_data
                except Exception:
                    pass
        except Exception:
            pass
        return filing_history

    def close(self):
        if self.driver:
            self.driver.quit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def load_existing_results(csv_output_path):
    """Load already scraped GSTINs to avoid duplication on restart."""
    existing_results = set()
    if os.path.exists(csv_output_path):
        with open(csv_output_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            existing_results.update(row["gstin"]
                                    for row in reader if "gstin" in row)
    return existing_results


def process_csv(csv_file_path, csv_output_path, remote=False):
    scraped_gstins = load_existing_results(csv_output_path)

    with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        gst_list = [row['gstin'] for row in reader if row['gstin']
                    and row['gstin'] not in scraped_gstins]

    if not gst_list:
        logging.info("No new GSTINs to scrape. Exiting.")
        return

    try:
        with GSTScraper(remote=remote) as scraper:
            with open(csv_output_path, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = ["gstin", "Legal Name", "Trade Name", "Effective Date", "Constitution",
                              "GSTIN Status", "Taxpayer Type", "Principal Place", "goods_services", "filing_history"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                if os.stat(csv_output_path).st_size == 0:
                    writer.writeheader()

                for gstin in gst_list:
                    try:
                        data = scraper.parse_page(gstin)
                        if data:
                            writer.writerow(data)
                            logging.info(f"Scraped: {gstin}")
                    except Exception as e:
                        logging.error(f"Error processing {gstin}: {e}")

    except KeyboardInterrupt:
        logging.warning("Script interrupted by user. Progress saved.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")


def main(csv_file_path, csv_output_path):
    process_csv(csv_file_path, csv_output_path, remote=True)
    logging.info(f"Scraping completed. Results saved to {csv_output_path}")
