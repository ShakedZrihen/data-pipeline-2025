from extractProcess.saveToSqlProcess.normalize import normalize_envelope_strict, normalize_price_item, normalize_promo_item
from extractProcess.saveToSqlProcess.enrich import enrich_message
from extractProcess.saveToSqlProcess.validate import validate_message
from extractProcess.saveToSqlProcess.dlq import send_to_dlq

import os
import json
from datetime import datetime

def save_debug_json(msg: dict, tag="ok"):
    os.makedirs("debug_output", exist_ok=True)
    filename = f"debug_output/{tag}_{datetime.now().isoformat().replace(':', '-')}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(msg, f, ensure_ascii=False, indent=2)
    print(f"[debug] saved 1 item to {filename}")


def process_item(doc: dict, item: dict):

    env = normalize_envelope_strict(doc)

    t = env.get("type")
    if t == "pricesFull":
        msg = normalize_price_item(env, item)
    elif t == "promoFull":
        msg = normalize_promo_item(env, item)
    else:
        send_to_dlq({"doc": doc, "item": item}, f"unknown type: {t}", stage="normalize")
        return None

    msg = enrich_message(msg)

    ok, err = validate_message(msg)
    if not ok:
        send_to_dlq(msg, err, stage="validation")
        return None

    save_debug_json(msg, tag="ok")

    return msg
