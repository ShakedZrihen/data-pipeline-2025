import os
import json
from datetime import datetime
from extractProcess.saveToSqlProcess.normalize import normalize_envelope_strict, normalize_price_item, normalize_promo_item
from extractProcess.saveToSqlProcess.validate import validate_message
from extractProcess.saveToSqlProcess.dlq import send_to_dlq
from extractProcess.saveToSqlProcess.enrich import enrich_brand_itemtype
from extractProcess.saveToSqlProcess.store import persist_message


# for me cause i wanted to check the files
def save_debug_json(msg: dict, tag="ok", max_files_per_type=30):
    os.makedirs("debug_output", exist_ok=True)
    t = msg.get("type", "unknown")
    filename = f"debug_output/{t}_{tag}_{datetime.now().isoformat().replace(':', '-')}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(msg, f, ensure_ascii=False, indent=2)

    files = sorted(
        [os.path.join("debug_output", fn) for fn in os.listdir("debug_output") if fn.startswith(t + "_")],
        key=os.path.getmtime
    )
    if len(files) > max_files_per_type:
        for old_file in files[:-max_files_per_type]:
            os.remove(old_file)


def process_item(doc: dict, item: dict):
    env = normalize_envelope_strict(doc)
    t = env.get("type")

    enriched_item = enrich_brand_itemtype(env, dict(item))

    if t == "pricesFull":
        msg = normalize_price_item(env, enriched_item)
    elif t == "promoFull":
        msg = normalize_promo_item(env, enriched_item)
    else:
        send_to_dlq({"doc": doc, "item": item}, f"unknown type: {t}", stage="normalize")
        return None

    ok, err = validate_message(msg)
    if not ok:
        send_to_dlq(msg, err, stage="validation")
        return None

    try:
        persist_message(msg)
    except Exception as e:
        send_to_dlq({"msg": msg}, f"db persist error: {e}", stage="db")
        return None

    # save_debug_json(msg, tag="ok")

    return msg
