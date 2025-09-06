import os
import xml.etree.ElementTree as ET
import json

STORES_DIR = os.path.dirname(os.path.abspath(__file__))

def get_text(root, *tags):
    """
    מחזיר טקסט של תגית ראשונה שנמצאה מבין האפשרויות.
    למשל get_text(root, "ChainId", "ChainID")
    """
    for tag in tags:
        el = root.find(tag)
        if el is not None and el.text is not None:
            return el.text.strip()
    return ""  # אם לא נמצא כלום

def xml_to_json(xml_file, json_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    provider_id = get_text(root, "ChainId", "ChainID")
    provider_name = get_text(root, "ChainName")

    # ✅ תיקון שם מותג: "מגה בעיר" → "קרפור"
    if provider_name == "מגה בעיר":
        provider_name = "קרפור"

    branches = {}
    for store in root.findall(".//Store"):
        store_id = get_text(store, "StoreId", "StoreID")
        branches[store_id] = {
            "name": get_text(store, "StoreName"),
            "address": get_text(store, "Address"),
            "city": get_text(store, "City")
        }

    data = {
        "provider_id": provider_id,
        "provider_name": provider_name,
        "branches": branches
    }

    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    for fname in os.listdir(STORES_DIR):
        if fname.endswith(".xml"):
            xml_path = os.path.join(STORES_DIR, fname)
            json_path = os.path.splitext(xml_path)[0] + ".json"
            xml_to_json(xml_path, json_path)
            print(f"✔ {fname} -> {os.path.basename(json_path)}")

if __name__ == "__main__":
    main()
