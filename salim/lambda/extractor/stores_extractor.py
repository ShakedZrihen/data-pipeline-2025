import json
import xml.etree.ElementTree as ET

def stores_extraction():

    providers = ["OsherAd", "TivTaam", "Yohananof"]
    stores_mapping = {}

    for provider in providers:
        tree = ET.parse(f"stores/{provider}.xml")
        root = tree.getroot()

        stores = root.findall(".//Store")
        id_to_name = {}

        for store in stores:
            store_id_1 = store.find("StoreID")
            store_id_2 = store.find("StoreId")

            if store_id_1 is not None:
                store_id = store_id_1
            else:
                store_id = store_id_2

            store_name = store.find("StoreName")

            if store_id is not None and store_name is not None:
                id_to_name[store_id.text.strip().zfill(3)] = store_name.text.strip()

        stores_mapping[provider] = id_to_name

    with open("stores/stores_mapping.json", "w", encoding="utf-8") as file:
        json.dump(stores_mapping, file, ensure_ascii=False, indent=4)

if __name__ == '__main__':
    stores_extraction()