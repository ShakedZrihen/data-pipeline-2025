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
    match = re.search(r'(\w+)(\d{13}-\d{3})-(\d{8})(\d{4})\.gz', filename)
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
    NOTE: This logic is based on a general assumption of the XML structure.
    You might need to adjust the find() and findall() calls to match the exact structure
    of the XML files from Rami Levi.
    """
    data = []
    try:
        root = ET.fromstring(xml_content)
        # Find all 'Item' elements within the XML
        for item in root.findall('.//Item'):
            item_code = item.find('ItemCode').text if item.find('ItemCode') is not None else None
            item_price = item.find('ItemPrice').text if item.find('ItemPrice') is not None else None
            
            # Extract promo data if the file is a promo file
            promo_details = {}
            if file_info['type'] == 'promo':
                promo_details['promotion_id'] = item.find('PromotionId').text if item.find('PromotionId') is not None else None
                # Add more promo-specific fields as needed

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
    Main function to run the extraction process on all downloaded files in a folder.
    """
    all_extracted_data = []
    
    if not os.path.exists(input_folder):
        print(f"Input folder '{input_folder}' not found. Cannot run extractor.")
        return

    for filename in os.listdir(input_folder):
        if filename.endswith(".gz"):
            filepath = os.path.join(input_folder, filename)
            print(f"Processing {filename}...")

            file_info = parse_filename(filename)
            if not file_info:
                print(f"Could not parse filename: {filename}")
                continue

            xml_content = process_gz_file(filepath)
            if not xml_content:
                continue
            
            extracted_data = extract_data_from_xml(xml_content, file_info)
            all_extracted_data.extend(extracted_data)
            
    # Save all extracted data to a single JSON file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_extracted_data, f, ensure_ascii=False, indent=2)

    print(f"Extraction complete. {len(all_extracted_data)} items saved to {output_file}.")

if __name__ == "__main__":
    # The timestamp needs to match the folder name created by the crawler.
    # It's best to pass this value from the crawler script.
    # Here, we'll assume the folder name is based on today's date.
    
    # Example usage:
    SLUG = "ramilevi"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M") # Use the same timestamp as the crawler
    input_folder_path = os.path.join("out", f"{SLUG}_{ts}")
    output_file_path = os.path.join("out", f"{SLUG}_extracted_data_{ts}.json")
    
    # Replace this with the actual folder name created by your crawler.
    # For example:
    # input_folder_path = os.path.join("out", "ramilevi_20250811_1732")
    
    run_extractor(input_folder_path, output_file_path)