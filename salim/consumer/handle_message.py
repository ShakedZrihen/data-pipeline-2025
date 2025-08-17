import json
import os
import sys
from dotenv import load_dotenv
from supabase import create_client
from time import sleep

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from salim.consumer.logger import logger
from salim.consumer.dlq_handler import send_to_dlq

# Load environment variables for Supabase
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Helper functions ---
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


# --- prices functions ---

def normalize_prices(body: dict) -> list:
    """
    Convert raw message body to a unified schema list of items.
    """
    items = body.get("root", {}).get("items", [])
    normalized_items = []
    for item in items:
        normalized_items.append({
            "store_id": body["root"].get("store_id"),
            "item_code": item.get("item_code"),
            "item_name": item.get("item_name"),
            "item_type": item.get("item_type"),
            "manufacturer_name": item.get("manufacturer_name"),
            "manufacture_country": item.get("manufacture_country"),
            "manufacturer_item_description": item.get("manufacturer_item_description"),
            "unit_qty": item.get("unit_qty"),
            "quantity": float(item.get("quantity", 0)),
            "unit_of_measure": item.get("unit_of_measure"),
            "b_is_weighted": int(item.get("b_is_weighted", 0)),
            "qty_price": float(item.get("qty_price", 0)),
            "unit_of_measure_price": float(item.get("unit_of_measure_price", 0)),
            "allow_discount": int(item.get("allow_discount", 0)),
            "item_status": int(item.get("item_status", 0)),
            "item_id": item.get("item_id") or None,
            "price_update_date": item.get("price_update_date")
        })
    return normalized_items

def enrich_prices(items: list) -> list:
    """
    Add missing or derived fields, e.g., source or timestamps.
    """
    for item in items:
        item["source"] = "SQS"
    return items

def validate_prices(items: list):
    """
    Basic validation - can extend with full schema rules.
    """
    for item in items:
        if not item.get("item_code"):
            raise ValueError("item_code is missing")
        if not item.get("store_id"):
            raise ValueError("store_id is missing")

def persist(items: list):
    """
    Upsert items into Supabase to avoid duplicates.
    """
    for item in items:
        supabase.table("items").insert(item).execute()

# --- promotions functions ---
# maya!!!

# --- Main function to handle a single message ---
def handle_message(raw_msg):
    try:
        body = json.loads(raw_msg["Body"])
        logger.debug(f"Raw message: {body}")
        if _has_promotions(body):
            logger.info("Message contains promotions. Skipping processing for now.") # maya!!
        else:
            normalized = normalize_prices(body)
            logger.debug(f"Normalized: {normalized}")

            enriched = enrich_prices(normalized)
            logger.debug(f"Enriched: {enriched}")

            validate_prices(enriched)
            persist(enriched)
            logger.info(f"Successfully processed {len(enriched)} items from store={enriched[0].get('store_id')}")
            return True

    except Exception as e:
        logger.exception("Failed to process message. Sending to DLQ.")
        # send_to_dlq(raw_msg, str(e))
        return False
