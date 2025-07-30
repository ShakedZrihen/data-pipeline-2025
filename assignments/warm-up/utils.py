from googletrans import Translator
import dateparser
import re

translator = Translator()

def translate_and_parse_date(date_str):
    try:
        if re.search(r'[\u0590-\u05FF]', date_str): # Hebrew Unicode range
            translated = translator.translate(date_str, src='iw', dest='en').text
            parsed = dateparser.parse(translated, languages=['en'])
        else:
            parsed = dateparser.parse(date_str)

        return parsed.isoformat() if parsed else date_str
    except Exception as e:
        print(f"Date parsing failed: {e}")
        return date_str
