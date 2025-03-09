
from utils.scraper import MicroTopicsIasscoreUrls, MicroTopicsIasscore
import json
import os


def main():
    def get_url():
        return "https://iasscore.in/upsc-syllabus/history/ancient-history"

    def extract(url):

        topics = []
        try:
            filepath = './data/microtopics.json'
            if os.path.exists(filepath):
                with open(filepath, 'r') as file:
                    topics = json.load(file)
            if not topics:
                scraper = MicroTopicsIasscoreUrls(base_url=url)
                scraper.scrape()
                for url in scraper.urls:
                    s = MicroTopicsIasscore(base_url=url)
                    s.scrape()
                    topics.extend(s.topics)
            convert_splitting_themes(topics)
            export_to_json(topics, filepath)
        except Exception as e:
            raise e

    def convert_splitting_themes(topics):
        split_topics = []
        for topic in topics[:]:
            if "\n" in topic.get("subtheme", ""):
                split_subthemes = topic["subtheme"].split("\n")
                topics.remove(topic)
                for split_subtheme in split_subthemes:
                    split_topic = dict(topic)
                    split_topic["subtheme"] = split_subtheme.strip()
                    split_topics.append(split_topic)
        topics.extend(split_topics)

    def export_to_json(data, filename):
        try:

            # Write to a JSON file
            with open(filename, mode="w", encoding="utf-8") as file:
                json.dump(data, file, indent=4)

            print(f"Topics successfully exported to {filename}")
        except Exception as e:
            print(f"Failed to export to JSON: {e}")

    extract(url=get_url())
