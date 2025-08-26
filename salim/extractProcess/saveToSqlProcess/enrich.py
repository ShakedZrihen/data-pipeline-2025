from extractProcess.extract import _clean

def enrich_common(msg: dict) -> dict:

    for k in ("provider","branch","type","product","unit","currency"):
        if k in msg and isinstance(msg[k], str):
            msg[k] = " ".join(msg[k].split()).strip()

    if not msg.get("currency"): # i want to consider to delete it because its always this default
        msg["currency"] = "ILS"

    if msg.get("branch") and msg["branch"].isdigit() and len(msg["branch"]) < 3:
        msg["branch"] = msg["branch"].zfill(3)

    if "productId" in msg and msg["productId"] is not None:
        msg["productId"] = str(msg["productId"]).strip()

    return msg

def enrich_prices(msg: dict) -> dict:
    if not msg.get("unit"):
        msg["unit"] = "unit"
    return msg

def enrich_promo(msg: dict) -> dict:
    try:
        if msg.get("unit") is None:
            msg["unit"] = 1
        elif float(msg["unit"]) <= 0:
            msg["unit"] = 1
    except Exception:
        msg["unit"] = 1
    return msg

def enrich_message(msg: dict) -> dict:
    msg = enrich_common(msg)
    if msg.get("type") == "pricesFull":
        return enrich_prices(msg)
    if msg.get("type") == "promoFull":
        return enrich_promo(msg)
    return msg
