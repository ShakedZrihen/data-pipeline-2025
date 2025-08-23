import json
import os
import sys
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from supabase import create_client

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from salim.consumer.logger import logger
from salim.consumer.dlq_handler import send_to_dlq

# ---------------- Env & Client ----------------
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------------- Utils ----------------
def _has_promotions(body) -> bool:
    """
    Return True if the message body contains a promotions array
    under root (case-insensitive). Supports: root.promotions / root.Promotions.
    """
    if not isinstance(body, dict):
        return False
    root = body.get("root") or body.get("Root")
    if not isinstance(root, dict):
        return False
    for k in ("promotions", "Promotions"):
        v = root.get(k)
        if isinstance(v, list) and len(v) > 0:
            return True
    return False


def _chunked(iterable: List[Dict[str, Any]], size: int):
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]

# ---------------- Prices ----------------
def normalize_prices(body: dict) -> List[Dict[str, Any]]:
    """
    Flatten root + item fields so each row (item) has ALL required columns.
    Returns a list[dict] ready for DB insert.
    """

    def to_int(v, default=None):
        try:
            if v in (None, ""):
                return default
            return int(v)
        except (ValueError, TypeError):
            return default

    def to_float(v, default=0.0):
        try:
            if v in (None, ""):
                return default
            return float(v)
        except (ValueError, TypeError):
            return default

    root = body.get("root", {}) or {}
    items = root.get("items", []) or []

    rows: List[Dict[str, Any]] = []
    for item in items:
        # Defaults
        manufacturer_name = item.get("manufacturer_name") or "לא ידוע"
        manufacture_country = item.get("manufacture_country") or "לא ידוע"
        unit_of_measure = item.get("unit_of_measure") or "יחידה"

        # Prefer item-level price_update_date, fallback to root-level
        price_update_date = item.get("price_update_date") or root.get("price_update_date")

        row = {
            # --- Item fields ---
            "price_update_date": price_update_date,
            "item_code": item.get("item_code"),
            "item_type": item.get("item_type"),
            "item_name": item.get("item_name"),
            "manufacturer_name": manufacturer_name,
            "manufacture_country": manufacture_country,
            "manufacturer_item_description": item.get("manufacturer_item_description"),
            "unit_qty": item.get("unit_qty"),
            "quantity": to_float(item.get("quantity"), 0.0),
            "unit_of_measure": unit_of_measure,
            "b_is_weighted": to_int(item.get("b_is_weighted"), 0),
            "qty_price": to_float(item.get("qty_price"), 0.0),
            "unit_of_measure_price": to_float(item.get("unit_of_measure_price"), 0.0),
            "allow_discount": to_int(item.get("allow_discount"), 0),
            "item_status": to_int(item.get("item_status")),  # If conversion fails -> None
            "item_id": item.get("item_id") or None,

            # --- Root fields (duplicated per row) ---
            "chain_id": root.get("chain_id"),
            "sub_chain_id": root.get("sub_chain_id"),
            "store_id": root.get("store_id"),
            "store_address": root.get("store_address"),
            "store_city": root.get("store_city"),
            "bikoret_no": root.get("bikoret_no"),
            "company_id": root.get("company_id"),
            "company_name": root.get("company_name"),
        }
        rows.append(row)

    return rows


def validate_prices(items: List[Dict[str, Any]]):
    """
    Basic validation for prices rows.
    """
    for item in items:
        if not item.get("item_code"):
            raise ValueError("item_code is missing")
        if not item.get("store_id"):
            raise ValueError("store_id is missing")


def persist_prices(items: List[Dict[str, Any]], batch_size: int = 500):
    """
    Insert prices in batches. Will raise on UNIQUE/PK conflicts.
    """
    if not items:
        return
    for chunk in _chunked(items, batch_size):
        supabase.table("prices").insert(chunk).execute()

# ---------------- Promotions ----------------
def normalize_promotions(body: dict) -> List[Dict[str, Any]]:
    """
    Flatten root + promotion fields so each DB row contains ALL required columns,
    including one row per promotion_item (ItemCode/ItemType/IsGiftItem/GiftItemPrice).
    If a promotion has no promotion_items, one row is emitted with item fields = None.
    """

    def to_int(v, default: Optional[int] = None) -> Optional[int]:
        try:
            if v in (None, ""):
                return default
            if isinstance(v, bool):
                return 1 if v else 0
            if isinstance(v, str) and v.strip().lower() in {"true", "t", "yes", "y"}:
                return 1
            if isinstance(v, str) and v.strip().lower() in {"false", "f", "no", "n"}:
                return 0
            return int(v)
        except (ValueError, TypeError):
            return default

    def to_float(v, default: float = 0.0) -> float:
        try:
            if v in (None, ""):
                return default
            return float(v)
        except (ValueError, TypeError):
            return default

    def to_str(v, default: Optional[str] = None) -> Optional[str]:
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return default
        return str(v)

    root = body.get("root", {}) or {}
    promotions = root.get("promotions", []) or []

    rows: List[Dict[str, Any]] = []

    for promo in promotions:
        # Additional restrictions (object)
        ar = promo.get("additional_restrictions", {}) or {}
        additional_is_active = to_int(ar.get("AdditionalIsActive"), None)
        additional_is_coupon = to_int(ar.get("AdditionalIsCoupon"), None)
        additional_is_total = to_int(ar.get("AdditionalIsTotal"), None)
        additional_gift_count = to_int(ar.get("AdditionalGiftCount"), None)
        additional_min_amount = to_float(ar.get("AdditionalMinAmount"), 0.0)

        # Base promotion fields
        base_row = {
            "promotion_update_date": promo.get("promotion_update_date") or root.get("promotion_update_date"),
            "allow_multiple_discounts": to_int(promo.get("allow_multiple_discounts"), 0),
            "promotion_id": to_str(promo.get("promotion_id")),
            "promotion_description": to_str(promo.get("promotion_description")),
            "promotion_start_date": to_str(promo.get("promotion_start_date")),
            "promotion_start_hour": to_str(promo.get("promotion_start_hour")),
            "promotion_end_date": to_str(promo.get("promotion_end_date")),
            "promotion_end_hour": to_str(promo.get("promotion_end_hour")),
            "min_no_of_item_offered": to_int(promo.get("min_no_of_item_offered"), None),
            "club_id": to_str(promo.get("club_id")),
            "reward_type": to_str(promo.get("reward_type")),
            "is_weighted_promo": to_int(promo.get("is_weighted_promo"), 0),

            # Additional restrictions (flattened)
            "additional_is_active": additional_is_active,
            "additional_is_coupon": additional_is_coupon,
            "additional_is_total": additional_is_total,
            "additional_gift_count": additional_gift_count,
            "additional_min_amount": additional_min_amount,

            "min_qty": to_int(promo.get("min_qty"), None),
            "max_qty": to_int(promo.get("max_qty"), None),
            "min_purchase_amount": to_float(promo.get("min_purchase_amount"), 0.0),
            "discount_type": to_str(promo.get("discount_type")),
            "discount_rate": to_float(promo.get("discount_rate"), 0.0),
            "remarks": to_str(promo.get("remarks")),
            "clubs": promo.get("clubs"),  # keep as-is (dict/list); store in JSONB column in DB

            # Root fields (duplicated on every row)
            "chain_id": to_str(root.get("chain_id")),
            "sub_chain_id": to_str(root.get("sub_chain_id")),
            "store_id": to_str(root.get("store_id")),
            "store_address": to_str(root.get("store_address")),
            "store_city": to_str(root.get("store_city")),
            "bikoret_no": to_str(root.get("bikoret_no")),
            "company_id": to_str(root.get("company_id")),
            "company_name": to_str(root.get("company_name")),
        }

        # Process promotion_items into a clean array
        items = promo.get("promotion_items", []) or []
        promotion_items = []
        for item in items:
            promotion_items.append({
                "item_code": to_str(item.get("ItemCode")),
                "item_type": to_str(item.get("ItemType")), 
                "is_gift_item": to_int(item.get("IsGiftItem"), None),
                "gift_item_price": to_float(item.get("GiftItemPrice"), 0.0)
            })

        # Add promotion_items as JSONB array to base_row
        base_row["promotion_items"] = promotion_items
        
        # Add single row per promotion (not per item)
        rows.append(base_row)

    return rows


def validate_promotions(rows: List[Dict[str, Any]]):
    """
    Basic validation for promotions rows.
    """
    for r in rows:
        if not r.get("promotion_id"):
            raise ValueError("promotion_id is missing")
        if not r.get("store_id"):
            raise ValueError("store_id is missing")


def persist_promotions(rows: List[Dict[str, Any]], batch_size: int = 500):
    """
    Insert promotions in batches. Will raise on UNIQUE/PK conflicts.
    """
    if not rows:
        return
    for chunk in _chunked(rows, batch_size):
        supabase.table("promotions").insert(chunk).execute()

# ---------------- Main ----------------
def handle_message(raw_msg) -> bool:
    try:
        body = json.loads(raw_msg["Body"])
        logger.debug(f"Raw message: {body}")

        if _has_promotions(body):
            normalized_promotions = normalize_promotions(body)
            logger.debug(f"Normalized (promotions): {normalized_promotions}")

            validate_promotions(normalized_promotions)
            persist_promotions(normalized_promotions)

            if normalized_promotions:
                logger.info(
                    f"Successfully processed {len(normalized_promotions)} promotions "
                    f"from store={normalized_promotions[0].get('store_id')}"
                )
            return True

        # else -> prices
        normalized_prices = normalize_prices(body)
        logger.debug(f"Normalized (prices): {normalized_prices}")

        validate_prices(normalized_prices)
        persist_prices(normalized_prices)

        if normalized_prices:
            logger.info(
                f"Successfully processed {len(normalized_prices)} items "
                f"from store={normalized_prices[0].get('store_id')}"
            )
        return True

    except Exception as e:
        logger.exception("Failed to process message. Sending to DLQ.")
        send_to_dlq(raw_msg, str(e))
        return False
