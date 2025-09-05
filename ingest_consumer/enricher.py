import re
import sqlite3

HEB_SPACE = re.compile(r"\s+")
PUNCT = re.compile(r"[^\w\u0590-\u05FF%]+", re.UNICODE)

def canonical_name(s: str) -> str:
    s = s.strip()
    s = HEB_SPACE.sub(" ", s)
    s = PUNCT.sub(" ", s).strip()
    return s

def parse_unit(unit: str | None):
    if not unit:
        return ("unit", 1.0, 1.0)
    u = unit.strip().lower()
    if "100" in u and "גר" in u:
        return ("g", 1/100.0, 100.0)
    if u in ("liter","ליטר","ליטר ","liter "):
        return ("ml", 1/1000.0, 1000.0)
    if u in ("unit","יח","יחידה","יחידות"):
        return ("unit", 1.0, 1.0)
    return ("unit", 1.0, 1.0)

def price_per_std(price: float, unit_std: str, factor_to_std: float) -> float:
    if unit_std == "g":
        return price * (100.0 * factor_to_std)
    if unit_std == "ml":
        return price * (1000.0 * factor_to_std)
    return price
def enrich_message(con: sqlite3.Connection, message_id: int):
    cur = con.cursor()
    rows = cur.execute(
        "SELECT product, price, unit FROM items WHERE message_id=?",
        (message_id,)
    ).fetchall()

    to_ins = []
    for (product, price, unit) in rows:
        name = canonical_name(product or "")
        unit_std, factor_to_std, base_amount = parse_unit(unit or "")
        ppu = price_per_std(float(price), unit_std, factor_to_std)
        to_ins.append((
            message_id, product, name, None, unit, unit_std, float(price), float(ppu), None
        ))

    cur.executemany(
        "INSERT INTO enriched_items(message_id, product_raw, product_name, qty, qty_unit, unit_std, price, price_per_unit, meta) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        to_ins
    )
    con.commit()
    return len(to_ins)
