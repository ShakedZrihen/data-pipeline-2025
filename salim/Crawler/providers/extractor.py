import os
import gzip
import re
import xml.etree.ElementTree as ET
import json
from datetime import datetime, timezone

def parse_filename(filename):
    """
    Parses a filename to extract relevant information such as file type, store ID, date, and time.
    Filename example: Price7290058140886-001-202508110700.gz
    """
    # Regex מתאים גם לשמות קבצים עם Promo וגם עם Price
    match = re.search(r'(\w+?)(\d{13}-\d{3})-(\d{8})(\d{4})', filename)
    if match:
        type_prefix = match.group(1)
        store_id = match.group(2)
        date = match.group(3)
        time = match.group(4)
        return {
            'type': 'price' if 'price' in type_prefix.lower() else 'promo',
            'store_id': store_id,
            'date': date,
            'time': time
        }
    return None

def process_gz_file(filepath):
    """Reads and decodes a gzip compressed file, returning its content as a string."""
    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        return None

def extract_data_from_xml(xml_content, file_info):
    """
    Parses XML content and extracts product data into a list of dictionaries.
    """
    data = []
    try:
        root = ET.fromstring(xml_content)
        # Namespace יכול להשתנות בין קבצים, לכן שימוש ב-wildcard הוא גישה טובה
        items_xpath = ".//{*}Item"
        
        for item in root.findall(items_xpath):
            # מציאת אלמנטים עם תמיכה ב-namespace
            def find_text(element, tag_name):
                found = element.find(f"{{*}}{tag_name}")
                return found.text if found is not None else None

            item_code = find_text(item, 'ItemCode')
            item_price = find_text(item, 'ItemPrice')
            
            promo_details = {}
            if file_info['type'] == 'promo':
                promo_details['promotion_id'] = find_text(item, 'PromotionId')
                # אפשר להוסיף כאן חילוץ של פרטי מבצע נוספים אם צריך

            if item_code and (item_price or file_info['type'] == 'promo'):
                record = {
                    'item_code': item_code,
                    'store_id': file_info['store_id'],
                    'file_date': file_info['date'],
                    'file_type': file_info['type']
                }
                if item_price:
                    record['price'] = float(item_price)
                if promo_details:
                    record.update(promo_details)
                
                data.append(record)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
    return data

def run_extractor(input_folder, output_file):
    """
    Main function to run the extraction process on all downloaded files.
    Walks through subdirectories to find all .gz files.
    """
    all_extracted_data = []
    
    if not os.path.exists(input_folder):
        print(f"Input folder '{input_folder}' not found. Cannot run extractor.")
        return

    # <<< השינוי המרכזי: שימוש ב-os.walk במקום os.listdir >>>
    # הלולאה הזו תעבור על תיקיית הבסיס ועל כל תתי-התיקיות שבתוכה.
    for dirpath, _, filenames in os.walk(input_folder):
        for filename in filenames:
            if filename.endswith(".gz"):
                # בניית נתיב מלא לקובץ
                filepath = os.path.join(dirpath, filename)
                print(f"Processing {filepath}...")

                file_info = parse_filename(filename)
                if not file_info:
                    print(f"Could not parse filename: {filename}")
                    continue

                xml_content = process_gz_file(filepath)
                if not xml_content:
                    continue
                
                extracted_data = extract_data_from_xml(xml_content, file_info)
                all_extracted_data.extend(extracted_data)
                
    # שמירת כל המידע שחולץ לקובץ JSON אחד
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_extracted_data, f, ensure_ascii=False, indent=2)

    print(f"Extraction complete. {len(all_extracted_data)} items saved to {output_file}.")

if __name__ == "__main__":
    # הגדרת שם הספק (slug) ותיקיית הקלט
    # לדוגמה, עבור יוחננוף
    SLUG = "yohananof" 
    
    # <<< עדכון נתיב תיקיית הקלט כדי שיתאים למבנה החדש >>>
    # התיקייה היא עכשיו פשוט 'out/yohananof' ולא כוללת חותמת זמן
    input_folder_path = os.path.join("out", SLUG)
    
    # קובץ הפלט עדיין יכול להכיל חותמת זמן כדי למנוע דריסה של קבצים ישנים
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    output_file_path = os.path.join("out", f"{SLUG}_extracted_data_{ts}.json")
    
    print(f"Running extractor for '{SLUG}'...")
    print(f"Input folder: {input_folder_path}")
    print(f"Output file: {output_file_path}")
    
    run_extractor(input_folder_path, output_file_path)