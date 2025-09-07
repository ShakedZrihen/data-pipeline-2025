import os, re, json, time, logging, sqlite3
from typing import Optional, Tuple

DB_PATH = os.environ.get("DB_PATH", os.path.join("data", "prices.db"))
SLEEP_SEC = int(os.environ.get("ENRICHER_SLEEP", "0"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

UNIT_MAP = {
    "ק\"ג": "kg", "קג": "kg", "קילוגרם": "kg",
    "גרם": "g", "ג'": "g", "100 גרם": "g100",
    "ליטר": "l", "100 מ\"ל": "ml100", "מ\"ל": "ml",
    "יח'": "unit", "יחידות": "unit", "יחידה": "unit",
    "Unknown": None, None: None, "": None
}

CATEGORIES = [
    (r"\bחלב\b|\b3%\b|\bשוקו\b", "מוצרי חלב"),
    (r"\bגבינה\b|\bקוטג\b", "גבינות"),
    (r"\bבירה\b|\bיין\b|\bוודקה\b", "אלכוהול"),
    (r"\bעוג(ה|יות)\b|\bביסקוויט\b|\bוופל\b", "ממתקים/עוגיות"),
    (r"\bעוף\b|\bבקר\b|\bסטייק\b|\bנקניק\b", "בשר/עוף"),
    (r"\bסלמון\b|\bטונה\b|\bדג\b", "דגים/ים"),
    (r"\bלחם\b|\bחלה\b|\bגבטה\b|\bבייגלה\b", "מאפייה"),
    (r"\bעגבנ(י|יה)\b|\bמלפפון\b|\bתפוח אדמה\b|\bקולורבי\b", "פירות/ירקות"),
]

def norm_unit(u: Optional[str]) -> Optional[str]:
    if u in UNIT_MAP:
        return UNIT_MAP[u]
    if isinstance(u, str):
        s = u.replace("גר'", "גרם").replace("גר", "גרם").replace("מ\"ל", "ml")
        for k, v in UNIT_MAP.items():
            if isinstance(k, str) and k in s:
                return v
    return None

def norm_name(name: str) -> str:
    s = re.sub(r"\s+", " ", name or "").strip()
    s = s.replace("™", "").replace("®", "")
    s = re.sub(r"[,.;:!?()\"'`]", "", s)
    return s

def guess_category(name: str) -> Optional[str]:
    s = name or ""
    for patt, cat in CATEGORIES:
        if re.search(patt, s, flags=re.IGNORECASE):
            return cat
    return None

def ensure_tables(conn: sqlite3.Connection):
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS enriched_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id INTEGER NOT NULL,
        product_norm TEXT,
        price REAL,
        unit_norm TEXT,
        meta_json TEXT,
        FOREIGN KEY(message_id) REFERENCES messages(id)
    );
    CREATE INDEX IF NOT EXISTS idx_enriched_msg ON enriched_items(message_id);
    """)

def enrich_once(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute("""
        SELECT i.id, i.message_id, i.product, i.price, i.unit
        FROM items i
        LEFT JOIN enriched_items e ON e.message_id = i.message_id
             AND e.product_norm = REPLACE(TRIM(LOWER(i.product)), '"', '')
             AND (e.price = i.price OR (e.price IS NULL AND i.price IS NULL))
        WHERE e.id IS NULL
        LIMIT 500
    """)
    rows = cur.fetchall()
    if not rows:
        return 0

    inserted = 0
    for _id, message_id, product, price, unit in rows:
        pnorm = norm_name(product).lower()
        unorm = norm_unit(unit)
        cat = guess_category(product)
        meta = {"orig_unit": unit, "category": cat}
        cur.execute("""
            INSERT INTO enriched_items(message_id, product_norm, price, unit_norm, meta_json)
            VALUES(?, ?, ?, ?, ?)
        """, (message_id, pnorm, price, unorm, json.dumps(meta, ensure_ascii=False)))
        inserted += 1

    conn.commit()
    logging.info(f"ENRICH: inserted={inserted}")
    return inserted

def main():
    while True:
        with sqlite3.connect(DB_PATH) as conn:
            ensure_tables(conn)
            n = enrich_once(conn)
        if SLEEP_SEC <= 0:
            break
        time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()
