import json

def convert_json_to_target_format(input_file_path, output_file_path):
    with open(input_file_path, "r", encoding="utf-8") as f:
        original_data = json.load(f)

    def safe_get(d, key, default="לא ידוע"):
        return d.get(key) if d.get(key) is not None else default

    def map_item(item):
        return {
            "price_update_date": safe_get(item, "PriceUpdateDate") or item.get("PriceUpdateTime"),
            "item_code": item.get("ItemCode"),
            "item_type": item.get("ItemType"),
            "item_name": item.get("ItemName"),
            "manufacturer_name": safe_get(item, "ManufacturerName", "לא ידוע") or safe_get(item, "ManufactureName", "לא ידוע"),
            "manufacture_country": safe_get(item, "ManufactureCountry", "לא ידוע"),
            "manufacturer_item_description": item.get("ManufacturerItemDescription") or item.get("ManufactureItemDescription"),
            "unit_qty": item.get("UnitQty"),
            "quantity": item.get("Quantity"),
            "unit_of_measure": item.get("UnitOfMeasure") or "יחידה",
            "b_is_weighted": item.get("bIsWeighted") or item.get("BisWeighted", "0"),
            "qty_price": item.get("ItemPrice"),
            "unit_of_measure_price": item.get("UnitOfMeasurePrice"),
            "allow_discount": item.get("AllowDiscount"),
            "item_status": item.get("ItemStatus"),
            "item_id": item.get("ItemId", "")
        }

    root = original_data["Root"]
    items = root["Items"]["Item"]

    transformed_data = {
        "Root": {
            "chain_id": root.get("ChainID") or root.get("ChainId"),
            "sub_chain_id": root.get("SubChainID") or root.get("SubChainId"),
            "store_id": root.get("StoreID") or root.get("StoreId"),
            "bikoret_no": root.get("BikoretNo"),
            "items": [map_item(item) for item in items],
            "@Count": str(len(items))
        }
    }

    with open(output_file_path, "w", encoding="utf-8") as f:
        json.dump(transformed_data, f, ensure_ascii=False, indent=2)

    print(f"✅ File saved to: {output_file_path}")
