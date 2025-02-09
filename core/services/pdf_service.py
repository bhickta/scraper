import fitz


class PDFService:
    def __init__(self, pdf_path):
        self.pdf_path = pdf_path

    def extract_text_dict(self, pages=None):
        extracted_text = {}
        try:
            doc = fitz.open(self.pdf_path)
            target_pages = range(doc.page_count) if pages is None else pages
            for page_num in target_pages:
                page = doc[page_num]
                text = page.get_text("text")
                extracted_text[page_num] = text
            doc.close()
        except Exception as e:
            print(f"Error opening or processing PDF: {e}")
            return None
        return extracted_text

    def extract_text_string(self, pages=None):
        extracted_text = ""
        try:
            doc = fitz.open(self.pdf_path)
            target_pages = range(doc.page_count) if pages is None else pages
            for page_num in target_pages:
                page = doc[page_num]
                text = page.get_text("text")
                extracted_text += text
            doc.close()
        except Exception as e:
            print(f"Error opening or processing PDF: {e}")
            return None
        return extracted_text

    def extract_text(self, pages=None):
        return self.extract_text_string(pages)
