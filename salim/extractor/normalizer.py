import json
import re
from datetime import datetime, timezone
from pathlib import Path

def detect_type_from_filename(filename):
    f = filename.lower()
    if "promo" in f:
        return "promoFull"
    if "price" in f or "prices" in f:
        return "pricesFull"
    return "unknown"

def parse_timestamp_from_filename(filename):
    match = re.search(r'(\d{8})[_-]?(\d{6})', filename)
    if match:
        ds, ts = match.groups()
        try:
            dt = datetime.strptime(ds + ts, "%Y%m%d%H%M%S")
            return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            return None
    return None

def to_float(var):
    if var is None:
        return None
    if isinstance(var, (int, float)):
        return float(var)
    str_var = str(var).strip().replace(",", "")
    try:
        return float(str_var)
    except Exception:
        str_var = re.sub(r"[^\d\.-]", "", str_var)
        try:
            return float(str_var)
        except Exception:
            return None

def clean_unit(unit):
    if not unit:
        return None
    str_var = str(unit).strip()
    if str_var.lower() in ("unknown", "unk", "none", "לא ידוע", "0"):
        return None
    if re.fullmatch(r"0+", str_var):
        return None
    return str_var

def parse_item_ts(s):
    if not s:
        return None
    s = str(s).replace("/", "-")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            pass
    return None

def normalize_price_item(item):
    name = (
        item.get("ItemNm")
        or item.get("ItemName")
        or item.get("ManufacturerItemDescription")
        or item.get("ItemDescription")
        or item.get("ItemDesc")
    )
    price = to_float(item.get("ItemPrice") or item.get("Price") or item.get("UnitOfMeasurePrice"))
    unit = clean_unit(item.get("UnitOfMeasure") or item.get("UnitQty"))
    qty = item.get("Quantity") or item.get("QtyInPackage")
    unit_str = f"{qty} {unit}" if unit and qty and str(qty).strip() not in ("0", "0.0", "0.00", "0.0000") else unit
    return {
        "product": name,
        "price": price,
        "unit": unit_str,
        "itemCode": item.get("ItemCode"),
        "updatedAt": parse_item_ts(item.get("PriceUpdateDate")),
    }

def normalize_promo(item):
    def to_iso(date_str, time_str):
        if not date_str:
            return None
        try:
            return datetime.strptime(
                date_str + " " + (time_str or "00:00:00"),
                "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
            except Exception:
                return None

    products = []
    for promo_item, promo_product in (("PromotionItems", "Item"), ("PromotedProducts", "Item")):
        var = item.get(promo_item)
        if isinstance(var, dict):
            it = var.get(promo_product)
            if isinstance(it, list):
                for i in it:
                    products.append({"itemCode": i.get("ItemCode"), "name": i.get("ItemName") or i.get("ItemNm")})
            elif isinstance(it, dict):
                i = it
                products.append({"itemCode": i.get("ItemCode"), "name": i.get("ItemName") or i.get("ItemNm")})

    return {
        "promotionId": item.get("PromotionId") or item.get("PromotionID"),
        "description": item.get("PromotionDescription") or item.get("PromotionDesc"),
        "start": to_iso(item.get("PromotionStartDate"), item.get("PromotionStartHour")),
        "end": to_iso(item.get("PromotionEndDate"), item.get("PromotionEndHour")),
        "discountedPrice": to_float(item.get("DiscountedPrice")),
        "discountRate": to_float(item.get("DiscountRate")),
        "minQty": to_float(item.get("MinQty") or item.get("MinNoOfItemOfered")),
        "products": products,
    }

def get_root(data):
    if isinstance(data, dict):
        for k in ("Root", "root"):
            if k in data:
                return data[k]
    return data

def normalize_file(json_path):
    path = Path(json_path)
    parts = path.parts

    provider, branch = "unknown", "unknown"
    if "work" in parts:
        try:
            idx = parts.index("work")
            provider = parts[idx + 1]
            branch = parts[idx + 2]
        except Exception:
            pass

    type = detect_type_from_filename(path.name)
    top_ts = parse_timestamp_from_filename(path.name)

    data = json.load(open(json_path, "r", encoding="utf-8"))
    root = get_root(data)

    items = []
    timestamp = top_ts

    if type == "pricesFull" or (isinstance(root, dict) and "Items" in root and isinstance(root["Items"], dict) and "Item" in root["Items"]):
        type = "pricesFull"
        it = root.get("Items", {}).get("Item")
        if isinstance(it, list):
            items = [normalize_price_item(x) for x in it]
        elif isinstance(it, dict):
            items = [normalize_price_item(it)]
        ts_values = [i.get("updatedAt") for i in items if i.get("updatedAt")]
        if ts_values:
            timestamp = max(ts_values)

    elif type == "promoFull" or (isinstance(root, dict) and "Promotions" in root and isinstance(root["Promotions"], dict) and "Promotion" in root["Promotions"]):
        type = "promoFull"
        pr = root.get("Promotions", {}).get("Promotion")
        if isinstance(pr, list):
            items = [normalize_promo(p) for p in pr]
        elif isinstance(pr, dict):
            items = [normalize_promo(pr)]
    else:
        type = "unknown"

    if not timestamp:
        timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    return {
        "provider": provider,
        "branch": branch,
        "type": type,
        "timestamp": timestamp,
        "items": items
    }

def _byte_len(obj):
    return len(json.dumps(obj, ensure_ascii=False).encode("utf-8"))

def _envelope_with_items(envelope, items_slice):
    return {**envelope, "items": items_slice}

def _split_large_promo(envelope, promo, max_bytes):
    """
    Split a single large promo (huge `products`) into multiple smaller promos.
    """
    products = promo.get("products")
    if not isinstance(products, list) or not products:
        yield _envelope_with_items(envelope, [promo])
        return

    base = {**promo, "products": []}
    n = len(products)
    i = 0
    while i < n:
        lo, hi, best = 1, (n - i), 1
        while lo <= hi:
            mid = (lo + hi) // 2
            shard = {**base, "products": products[i:i+mid]}
            cand = _envelope_with_items(envelope, [shard])
            sz = _byte_len(cand)
            if sz <= max_bytes:
                best = mid
                lo = mid + 1
            else:
                hi = mid - 1
        shard = {**base, "products": products[i:i+best]}
        yield _envelope_with_items(envelope, [shard])
        i += best

def chunk_for_sqs(normalized, max_bytes=240_000):
    items = normalized.get("items") or []
    if not items:
        yield normalized
        return

    i = 0
    n = len(items)
    while i < n:
        step = 1
        last_good = 0
        while True:
            candidate = _envelope_with_items(normalized, items[i:i+step])
            size = _byte_len(candidate)
            if size <= max_bytes:
                last_good = step
                step += 1
                if i + step > n:
                    break
            else:
                break

        if last_good == 0:
            big = items[i]
            split_done = False
            for shard in _split_large_promo(normalized, big, max_bytes):
                if _byte_len(shard) > max_bytes:
                    pass
                yield shard
                split_done = True
            i += 1
        else:
            candidate = _envelope_with_items(normalized, items[i:i+last_good])
            yield candidate
            i += last_good
