class TextService:
    def __init__(self, text_path):
        self.text_path = text_path

    def extract_text(self, pages=None):
        with open(self.text_path, "r", encoding="utf-8") as f:
            text = f.read()
        return text
