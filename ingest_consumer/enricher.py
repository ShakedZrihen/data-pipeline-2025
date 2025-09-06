import sqlite3
import re
from typing import Tuple, Union

def _normalize_qty_unit(u: str) -> Tuple[str, float]:
    if not u:
        return ("unit", 1.0)
    s = u.strip().lower()
    if s in ("l", "lt", "liter", "ליטר"):
        return ("ml", 1000.0)
    if s in ("ml", "מ״ל", "מל"):
        return ("ml", 1.0)
    if s in ("kg", "קג", "קילו", "קילוגרם"):
        return ("g", 1000.0)
    if s in ("g", "גרם"):
        return ("g", 1.0)
    m = re.search(r"(\d+(?:\.\d+)?)\s*גרם", s)
    if m:
        qty = float(m.group(1))
        return ("g", qty if qty > 0 else 1.0)
    return ("unit", 1.0)

def enrich_message(db: Union[str, sqlite3.Connection], message_id: int) -> int:
    owns_conn = False
    if isinstance(db, sqlite3.Connection):
        con = db
    else:
        con = sqlite3.connect(db)
        owns_conn = True

    try:
        cur = con.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS enriched_items(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER NOT NULL,
            product_name TEXT NOT NULL,
            price REAL NOT NULL,
            unit_std TEXT NOT NULL,
            price_per_unit REAL NOT NULL,
            product_raw TEXT,
            FOREIGN KEY(message_id) REFERENCES messages(id)
        )
        """)
        cur.execute("DROP INDEX IF EXISTS ux_enriched_msg_prod")
        cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_enriched_msg_prod_v2
        ON enriched_items(message_id, product_name, unit_std)
        """)

        cur.execute("SELECT product, price, unit FROM items WHERE message_id = ?", (message_id,))
        rows_src = cur.fetchall()

        cur.execute("DELETE FROM enriched_items WHERE message_id = ?", (message_id,))

        to_insert = []
        for product_raw, price, unit in rows_src:
            unit_std, qty = _normalize_qty_unit(unit or "")
            try:
                p = float(price)
            except Exception:
                continue
            qty = float(qty) if qty and qty > 0 else 1.0
            ppu = p / qty
            product_name = (product_raw or "").strip()
            to_insert.append((message_id, product_name, p, unit_std, ppu, product_raw))

        dedup = {}
        for rec in to_insert:
            key = (rec[0], rec[1], rec[3])
            dedup[key] = rec
        to_insert = list(dedup.values())

        if to_insert:
            cur.executemany("""
            INSERT OR REPLACE INTO enriched_items
              (message_id, product_name, price, unit_std, price_per_unit, product_raw)
            VALUES (?,?,?,?,?,?)
            """, to_insert)

        con.commit()
        return len(to_insert)
    finally:
        if owns_conn:
            con.close()
