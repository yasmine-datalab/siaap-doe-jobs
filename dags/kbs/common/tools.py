import re

CODE_PATTERN = r'\d{2}[-_ ][A-Z]{2}[-_ ][A-Z]{2,4}[-_ ]?\d{2}[A-Z\d]{1}'

def extract_tags(text: str):
    return re.findall(CODE_PATTERN, text)
    