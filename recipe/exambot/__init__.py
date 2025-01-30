from .db import GenericDatabase
from .scrape import ExamBot
import json


def main():
    db_url = "sqlite:///data/exambot.data.db"
    db = GenericDatabase(db_url)
    explainations = db.query_with_filters("explanations")
    json_file_path = "data/parsed_data.json"

    with open(json_file_path, mode="a", encoding="utf-8") as file:
        # If the file is empty, start the array
        file.seek(0, 2)  # Move the cursor to the end of the file
        if file.tell() == 0:
            file.write("[\n")  # Start the array
        
        for explaination in explainations:
            page_no, row_index, html = explaination
            scraper = ExamBot(content=html)
            data = scraper.scrape()
            data.update(
                {
                    "metadata": {
                        "page_no": page_no,
                        "row_index": row_index,
                    }
                }
            )
            print(data.get("metadata"))
            # Write each data object
            json.dump(data, file, ensure_ascii=False, indent=4)
            
            # Write a comma if it's not the last item
            if explaination != explainations[-1]:
                file.write(",\n")
        
        # Close the array
        file.write("\n]")


if __name__ == "__main__":
    main()
