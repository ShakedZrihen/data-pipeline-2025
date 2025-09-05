from typing import Any, Dict, List

def _norm_unit(u: str) -> str:
    u = u.strip().lower()
    mapping = {
        "100 גרם": "100g",
        "100 gram": "100g",
        "100 g": "100g",
        "גרם": "g",
        "ג": "g",
        "ק\"ג": "kg",
        "קג": "kg",
        "kg": "kg",
        "ליטר": "l",
        "liter": "l",
        "מ\"ל": "ml",
        "מל": "ml",
        "unit": "unit",
        "יחידה": "unit",
        "יח'": "unit",
    }
    return mapping.get(u, u)

def normalize(msg: Dict[str, Any]) -> Dict[str, Any]:
    b = msg.get("branch")
    msg["branch"] = str(b).strip() if b is not None else ""

    t = msg.get("type", "")
    msg["type"] = t.strip().lower()

    p = msg.get("provider", "")
    msg["provider"] = p.strip()

    items: List[Dict[str, Any]] = []
    for it in msg.get("items_sample", []):
        name = (it.get("product") or "").strip()
        price = float(it.get("price", 0))
        unit = _norm_unit(str(it.get("unit") or ""))

        if len(name) > 200:
            name = name[:200].rstrip() + "…"

        items.append({"product": name, "price": price, "unit": unit})

    msg["items_sample"] = items
    return msg
