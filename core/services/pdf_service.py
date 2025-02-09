import fitz


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
                extracted_text[page_num] = text

            doc.close()  # Close the PDF document
        except Exception as e:
            print(f"Error opening or processing PDF: {e}")
            return None  # Or handle the error as needed

        return extracted_text
