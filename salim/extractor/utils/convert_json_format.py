import json

_COMPANY_MAP = {
    "carrefour": {"id": 1, "name": "carrefour"},
    "keshet":    {"id": 2, "name": "keshet"},
    "osherad":   {"id": 3, "name": "osherad"},
    "ramilevi":  {"id": 4, "name": "ramilevi"},
    "tivtaam":   {"id": 5, "name": "tivtaam"},
    "yohananof": {"id": 6, "name": "yohananof"},
}

def _detect_company_from_path(path: str):
    p = (path or "").lower()
    for slug, meta in _COMPANY_MAP.items():
        name = (meta.get("name") or "").lower()
        if slug in p or (name and name in p):
            return meta
    return None

def _load_store_lookup(lookup_path: str) -> dict:
    try:
        with open(lookup_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _digits_only(s):
    return "".join(ch for ch in str(s) if ch.isdigit())

def _target_len_for_company(company_slug: str | None) -> int:
    return 4 if (company_slug or "").lower() == "carrefour" else 3

def _candidate_store_keys(company_slug: str | None, store_id, lookup_dict: dict) -> list[str]:
    if store_id is None:
        return []
    raw = str(store_id).strip()
    digs = _digits_only(raw)
    preferred = []
    if digs:
        preferred.append(digs.zfill(_target_len_for_company(company_slug)))
    if company_slug:
        stores = lookup_dict.get((company_slug or "").lower())
        if isinstance(stores, dict) and stores:
            lengths = {}
            for k in stores.keys():
                lengths[len(str(k))] = lengths.get(len(str(k)), 0) + 1
            dominant_len = max(lengths, key=lengths.get)
            if digs:
                dom = digs.zfill(dominant_len)
                if dom not in preferred:
                    preferred.append(dom)
    candidates = []
    for k in [*preferred, raw, digs, digs.zfill(3) if digs else None, digs.zfill(4) if digs else None]:
        if k and k not in candidates:
            candidates.append(k)
    return candidates

def _get_store_address_city(lookup_dict: dict, company_slug: str | None, store_id) -> tuple[str | None, str | None]:
    if not company_slug:
        return (None, None)
    stores_by_company = lookup_dict.get((company_slug or "").lower())
    if not isinstance(stores_by_company, dict):
        return (None, None)
    for key in _candidate_store_keys(company_slug, store_id, lookup_dict):
        rec = stores_by_company.get(key)
        if isinstance(rec, dict):
            return (rec.get("address"), rec.get("city"))
    return (None, None)

def convert_json_to_target_prices_format(input_file_path, output_file_path, stores_lookup_path: str | None = None):
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

    company = _detect_company_from_path(input_file_path)
    company_name = company["name"] if company else None

    chain_id     = root.get("ChainID") or root.get("ChainId")
    sub_chain_id = root.get("SubChainID") or root.get("SubChainId")
    store_id_raw = root.get("StoreID") or root.get("StoreId")
    store_id_norm = (_digits_only(store_id_raw).zfill(_target_len_for_company(company_name)) if store_id_raw is not None else None)
    bikoret_no   = root.get("BikoretNo")

    lookup_dict = _load_store_lookup(stores_lookup_path) if stores_lookup_path else {}
    store_address, store_city = _get_store_address_city(lookup_dict, company_name, store_id_norm)

    transformed_data = {
        "Root": {
            "chain_id": chain_id,
            "sub_chain_id": sub_chain_id,
            "store_id": store_id_norm,
            "store_address": store_address,
            "store_city": store_city,
            "bikoret_no": bikoret_no,
            "company_id": company["id"] if company else None,
            "company_name": company_name,
            "items": [map_item(item) for item in items],
            "@Count": str(len(items))
        }
    }

    with open(output_file_path, "w", encoding="utf-8") as f:
        json.dump(transformed_data, f, ensure_ascii=False, indent=2)

    print(f"✅ File saved to: {output_file_path}")

def convert_json_to_target_promos_format(input_file_path, output_file_path, stores_lookup_path: str | None = None):
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

    root = (original_data.get("Root") or original_data.get("root") or {})
    chain_id     = pick(root.get("ChainID"), root.get("ChainId"))
    sub_chain_id = pick(root.get("SubChainID"), root.get("SubChainId"))
    store_id_raw = pick(root.get("StoreID"), root.get("StoreId"))
    company      = _detect_company_from_path(input_file_path)
    company_name = company["name"] if company else None
    store_id_norm = (_digits_only(store_id_raw).zfill(_target_len_for_company(company_name)) if store_id_raw is not None else None)
    bikoret_no   = root.get("BikoretNo")

    lookup_dict = _load_store_lookup(stores_lookup_path) if stores_lookup_path else {}
    store_address, store_city = _get_store_address_city(lookup_dict, company_name, store_id_norm)

    promos_node = root.get("Promotions") or root.get("promotions") or {}
    promotions_list = to_list(promos_node.get("Promotion") or promos_node.get("promotion"))

    def map_promo(promo):
        promotion_update_date = pick(promo.get("PromotionUpdateDate"), promo.get("PromotionUpdateTime"))
        items = to_list((promo.get("PromotionItems") or {}).get("Item"))
        mapped_items = []
        for it in items:
            mapped_items.append({
                "ItemCode": it.get("ItemCode"),
                "ItemType": it.get("ItemType"),
                "IsGiftItem": it.get("IsGiftItem"),
                "GiftItemPrice": it.get("GiftItemPrice")
            })
        ar = promo.get("AdditionalRestrictions") or {}
        additional_restrictions = {
            "AdditionalIsActive": ar.get("AdditionalIsActive"),
            "AdditionalIsCoupon": ar.get("AdditionalIsCoupon"),
            "AdditionalIsTotal": ar.get("AdditionalIsTotal"),
            "AdditionalGiftCount": ar.get("AdditionalGiftCount"),
            "AdditionalMinAmount": ar.get("AdditionalMinAmount"),
        }
        min_no_of_item_offered = pick(promo.get("MinNoOfItemOffered"), promo.get("MinNoOfItemOfered"))
        return {
            "promotion_update_date": promotion_update_date,
            "allow_multiple_discounts": promo.get("AllowMultipleDiscounts"),
            "promotion_id": pick(promo.get("PromotionID"), promo.get("PromotionId")),
            "promotion_description": promo.get("PromotionDescription"),
            "promotion_start_date": promo.get("PromotionStartDate"),
            "promotion_start_hour": promo.get("PromotionStartHour"),
            "promotion_end_date": promo.get("PromotionEndDate"),
            "promotion_end_hour": promo.get("PromotionEndHour"),
            "min_no_of_item_offered": min_no_of_item_offered,
            "club_id": pick(promo.get("ClubID"), promo.get("ClubId")),
            "reward_type": promo.get("RewardType"),
            "is_weighted_promo": promo.get("IsWeightedPromo"),
            "additional_restrictions": additional_restrictions,
            "min_qty": promo.get("MinQty"),
            "max_qty": promo.get("MaxQty"),
            "min_purchase_amount": promo.get("MinPurchaseAmount"),
            "discount_type": promo.get("DiscountType"),
            "discount_rate": promo.get("DiscountRate"),
            "remarks": promo.get("Remarks") if "Remarks" in promo else None,
            "clubs": promo.get("Clubs") if "Clubs" in promo else None,
            "promotion_items": mapped_items
        }

    transformed = {
        "root": {
            "chain_id": chain_id,
            "sub_chain_id": sub_chain_id,
            "store_id": store_id_norm,
            "bikoret_no": bikoret_no,
            "company_id": company["id"] if company else None,
            "company_name": company_name,
            "store_address": store_address,
            "store_city": store_city,
            "promotions": [map_promo(p) for p in promotions_list],
            "@count": str(len(promotions_list))
        }
    }

    with open(output_file_path, "w", encoding="utf-8") as f:
        json.dump(transformed, f, ensure_ascii=False, indent=2)
