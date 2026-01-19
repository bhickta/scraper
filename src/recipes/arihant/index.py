import re


def process_text_file(input_file, output_csv):
    with open(input_file, 'r', encoding='utf-8') as file, open(output_csv, 'w', encoding='utf-8') as output:
        output.write("Category,Start,End\n")
        for line in file:
            line = line.strip()
            if not line:
                continue

            match = re.match(r"(.+?)\s+(\d+)-(\d+)", line)
            if match:
                category = match.group(1).strip()
                start = match.group(2).strip()
                end = match.group(3).strip()
                output.write(f"{category},{start},{end}\n")

    print(f"CSV file saved: {output_csv}")


# Usage
input_file = "input.txt"
output_csv = "output.csv"
process_text_file(input_file, output_csv)
