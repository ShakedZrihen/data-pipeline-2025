import gzip
import xml.etree.ElementTree as ET
import json
import os
from datetime import datetime

def extract_items_from_xml(xml_content):
    items = []
    try:
        root = ET.fromstring(xml_content)
        for item in root.findall('.//Item'):
            item_code = item.find('ItemCode').text if item.find('ItemCode') is not None else "N/A"
            item_name = item.find('ItemName').text if item.find('ItemName') is not None else "Unknown"
            item_price = item.find('ItemPrice').text if item.find('ItemPrice') is not None else 0.0

            items.append({
                "product_id": item_code,
                "product": item_name,
                "price": float(item_price),
                "unit": "unit" 
            })
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
    return items

def process_file_content(file_key, xml_content):
    path_parts = file_key.replace("\\", "/").split('/')
    provider = path_parts[0]
    branch = path_parts[1]
    filename = path_parts[2]

    file_type = "pricesFull" if "price" in filename.lower() else "promoFull"
    items_list = extract_items_from_xml(xml_content)
    final_json = {
      "provider": provider,
      "branch": branch, 
      "type": file_type,
      "timestamp": datetime.now().isoformat(),
      "items": items_list
    }
    return final_json

# החלק הזה מיועד אך ורק לבדיקה מקומית
if __name__ == "__main__":
    

    sample_file_path = "salim/Crawler/out/yohananof/001/PriceFull7290803800003-001-202508120010.gz"


    print(f"Attempting to test with file: {sample_file_path}")

    if not os.path.exists(sample_file_path):
        print("="*50)
        print(f"!!! ERROR: Sample file not found at the specified path.")
        print(f"Current working directory is: {os.getcwd()}")
        print("Please ensure the path is correct relative to the main project folder.")
        print("="*50)
    else:

        file_key_for_test = "/".join(sample_file_path.split('/')[2:]) 

        with gzip.open(sample_file_path, 'rt', encoding='utf-8') as f:
            content = f.read()
        
        result_json = process_file_content(file_key_for_test, content)

        print("\n--- 🟢 Local Test SUCCESS! ---")
        print(json.dumps(result_json, indent=2, ensure_ascii=False))