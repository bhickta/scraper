import csv
import json
from utils.scraper import Scraper
import os


class GST(Scraper):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def parse_page(self):
        print(self.soup)


def get_url(gstin):
    return f"https://app.signalx.ai/gstin-verification/{gstin}"


def set_url():
    filepath = './data/gst_details.csv'
    try:
        if os.path.exists(filepath):
            with open(filepath, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                data = [row for row in reader]
                for row in data:
                    row["url"] = get_url(row['gstin'].strip())
                return data
    except Exception as e:
        print(f"Error reading CSV: {e}")
    return []


def execute(url):
    scraper = GST(base_url=url)
    scraper.scrape()


def export_to_csv(data, filename):
    try:
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print(f"GST details successfully exported to {filename}")
    except Exception as e:
        print(f"Failed to export to CSV: {e}")


def main():
    data = set_url()
    if data:
        for entry in data:
            print(entry["url"])
            execute(entry["url"])
            return
        export_to_csv(data, './data/updated_gst_details.csv')


if __name__ == "__main__":
    main()
