import json

def convert_json_to_target_prices_format(input_file_path, output_file_path):
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

    root = original_data.get("Root") or original_data.get("root")
    if root is None:
        print(f"❌ Skipping file (missing 'Root' or 'root'): {input_file_path}")
        return
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


def convert_json_to_target_promos_format(input_file_path, output_file_path):
    with open(input_file_path, "r", encoding="utf-8") as f:
        original_data = json.load(f)

    def pick(*vals, default=None):
        for v in vals:
            if v is not None and v != "":
                return v
        return default

    def to_list(x):
        if x is None: return []
        if isinstance(x, list): return x
        if isinstance(x, dict): return [x]
        return []

    # --- Root ---
    root = (original_data.get("Root") or original_data.get("root") or {})
    chain_id     = pick(root.get("ChainID"),    root.get("ChainId"))
    sub_chain_id = pick(root.get("SubChainID"), root.get("SubChainId"))
    store_id     = pick(root.get("StoreID"),    root.get("StoreId"))
    bikoret_no   = root.get("BikoretNo")

    promos_node = root.get("Promotions") or root.get("promotions") or {}
    promotions_list = to_list(promos_node.get("Promotion") or promos_node.get("promotion"))

    # --- Map one promo ---
    def map_promo(promo):
        # מאחדים ל-promotion_update_date (לפי הרוב)
        promotion_update_date = pick(promo.get("PromotionUpdateDate"),
                                     promo.get("PromotionUpdateTime"))

        # Items תמיד לרשימה + מפתחות קיימים/NULL
        items = to_list((promo.get("PromotionItems") or {}).get("Item"))
        mapped_items = []
        for it in items:
            mapped_items.append({
                "ItemCode":     it.get("ItemCode"),
                "ItemType":     it.get("ItemType"),
                "IsGiftItem":   it.get("IsGiftItem"),
                "GiftItemPrice": it.get("GiftItemPrice")
            })

        # AdditionalRestrictions – מאחדים מפתחות מוכרים, חסר -> None
        ar = promo.get("AdditionalRestrictions") or {}
        additional_restrictions = {
            "AdditionalIsActive":   ar.get("AdditionalIsActive"),
            "AdditionalIsCoupon":   ar.get("AdditionalIsCoupon"),
            "AdditionalIsTotal":    ar.get("AdditionalIsTotal"),
            "AdditionalGiftCount":  ar.get("AdditionalGiftCount"),
            "AdditionalMinAmount":  ar.get("AdditionalMinAmount"),
        }

        # טעות כתיב Ofered/Offered -> min_no_of_item_offered
        min_no_of_item_offered = pick(promo.get("MinNoOfItemOffered"),
                                      promo.get("MinNoOfItemOfered"))

        return {
            "promotion_update_date":  promotion_update_date,
            "allow_multiple_discounts": promo.get("AllowMultipleDiscounts"),
            "promotion_id":            pick(promo.get("PromotionID"), promo.get("PromotionId")),
            "promotion_description":   promo.get("PromotionDescription"),
            "promotion_start_date":    promo.get("PromotionStartDate"),
            "promotion_start_hour":    promo.get("PromotionStartHour"),
            "promotion_end_date":      promo.get("PromotionEndDate"),
            "promotion_end_hour":      promo.get("PromotionEndHour"),
            "min_no_of_item_offered":  min_no_of_item_offered,
            "club_id":                 pick(promo.get("ClubID"), promo.get("ClubId")),
            "reward_type":             promo.get("RewardType"),
            "is_weighted_promo":       promo.get("IsWeightedPromo"),
            "additional_restrictions": additional_restrictions,
            "min_qty":                 promo.get("MinQty"),
            "max_qty":                 promo.get("MaxQty"),
            "min_purchase_amount":     promo.get("MinPurchaseAmount"),
            "discount_type":           promo.get("DiscountType"),
            "discount_rate":           promo.get("DiscountRate"),
            "remarks":                 promo.get("Remarks") if "Remarks" in promo else None,
            "clubs":                   promo.get("Clubs")   if "Clubs"   in promo else None,
            "promotion_items":         mapped_items
        }

    transformed = {
        "root": {
            "chain_id":     chain_id,
            "sub_chain_id": sub_chain_id,
            "store_id":     store_id,
            "bikoret_no":   bikoret_no,
            "promotions":   [map_promo(p) for p in promotions_list],
            "@count":       str(len(promotions_list))
        }
    }

    with open(output_file_path, "w", encoding="utf-8") as f:
        json.dump(transformed, f, ensure_ascii=False, indent=2)
