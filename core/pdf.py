import fitz
import re
import json


class PDFService:
    def __init__(self, pdf_path):
        self.pdf_path = pdf_path

    def extract_text(self, pages=None):
        extracted_text = {}
        try:
            doc = fitz.open(self.pdf_path)  # Open the PDF document
            target_pages = range(doc.page_count) if pages is None else pages
            for page_num in target_pages:
                page = doc[page_num]
                text = page.get_text("text")

                # Basic Cleaning (Optional but Recommended)
                if text:
                    # Replace newlines with spaces
                    text = text.replace('\n', ' ')
                    # Remove extra whitespace
                    text = re.sub(r'\s+', ' ', text).strip()

                extracted_text[page_num] = text

            doc.close()  # Close the PDF document
        except Exception as e:
            print(f"Error opening or processing PDF: {e}")
            return None  # Or handle the error as needed

        return extracted_text


class MCQExtractor:
    def __init__(self, pdf_service: PDFService):
        self.pdf_service = pdf_service
        self.mcqs = []

    def process_mcqs(self, text):
        mcq_pattern = re.compile(
            r"(\d+)\.\s+(.*?)\s*\(1\)(.*?)\s*\(2\)(.*?)\s*\(3\)(.*?)\s*(?:\(4\)|\(5\))"
        )

        matches = mcq_pattern.findall(text)
        for match in matches:
            question_no, question, a, b, c = map(str.strip, match)
            self.mcqs.append({
                "question_no": question_no,
                "question": question + " " + " ".join([a, b, c,]),
                "a": question,
                "b": a,
                "c": b,
                "d": c
            })

    def process_answers(self, text):
        answer_pattern = re.compile(
            r"(\d+)\.\s+\((\d)\)"
        )
        matches = answer_pattern.findall(text)
        self.answer_dict = {}
        for question_no, answer in matches:
            self.answer_dict[question_no] = answer
        print(len(self.answer_dict.items()))

    def to_json(self, output_path="mcqs.json"):
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.mcqs, f, indent=4, ensure_ascii=False)

    def run(self, pages=None):
        text = self.pdf_service.extract_text(pages)
        for page_num, page_text in text.items():
            self.process_mcqs(page_text)
        self.validate()
        self.to_json()
        return self.mcqs

    def validate(self):
        self.validate_serial_no_in_order()
        # self.validate_answer_serial_no_in_order()

    def validate_answer_serial_no_in_order(self):
        for i, mcq in enumerate(self.mcqs):
            if i+1 != int(mcq["question_no"]):
                raise Exception(
                    f"MCQ number {mcq['question_no']} is not in order")

    def validate_serial_no_in_order(self):
        expected_question_no = 1
        for mcq in self.mcqs:
            if mcq["question_no"] in ["83", "247", "616", "1085"]:
                print(f"Skipping {mcq['question_no']}")
                expected_question_no += 2
                continue

            # Convert once and use it
            actual_question_no = int(mcq["question_no"])
            if actual_question_no != expected_question_no:
                raise Exception(f"MCQ number {mcq['question_no']} is not in order. Expected {
                                expected_question_no}")

            expected_question_no += 1


if __name__ == "__main__":
    pdf_service = PDFService("../data/SSC English KIRAN 11600+.pdf")
    extractor = MCQExtractor(pdf_service)
    mcqs = extractor.run(pages=range(102, 138))

    # answers = extractor.run(pages=range(138, 140))
