from __future__ import annotations
import os
import psycopg
from datetime import datetime, timezone

# חיבור גלובלי שממוחזר בין הקריאות
_CONN = None

def _get_conn():
    global _CONN
    if _CONN is not None and not _CONN.closed:
        return _CONN
    host = os.getenv("POSTGRES_HOST", "db")
    db   = os.getenv("POSTGRES_DB", "salim_db")
    user = os.getenv("POSTGRES_USER", "postgres")
    pwd  = os.getenv("POSTGRES_PASSWORD", "postgres")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    _CONN = psycopg.connect(host=host, dbname=db, user=user, password=pwd, port=port)
    _CONN.autocommit = True
    return _CONN

def _iso_to_dt(iso_str: str) -> datetime:
    """
    ממיר ISO8601 ל-datetime פייתון. תומך ב-Z.
    """
    if not iso_str:
        #fallback: עכשיו ב-UTC
        return datetime.now(timezone.utc)
    s = iso_str.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    # fromisoformat תומך ב"+00:00"
    return datetime.fromisoformat(s)

def persist_message(msg: dict) -> None:
    """
    מקבל מסר מאומת (validate_message=True) וכותב לטבלה המתאימה:
    - pricesFull  -> prices_full
    - promoFull   -> promos_full  (מפה product->promo_desc, price->discounted_price, unit->min_qty)
    """
    t = (msg.get("type") or "").strip()
    if t not in ("pricesFull", "promoFull"):
        return  # לא אמור לקרות בשלב הזה

    provider   = msg.get("provider")
    branch     = msg.get("branch")
    ts         = _iso_to_dt(msg.get("timestamp"))
    product_id = msg.get("productId")

    # brand/itemType אופציונליים
    brand      = msg.get("brand")
    item_type  = msg.get("itemType")

    conn = _get_conn()
    with conn.cursor() as cur:
        if t == "pricesFull":
            # product_name בטבלה נקרא product, unit מחרוזת
            cur.execute(
                """
                INSERT INTO prices_full
                  (provider, branch, ts, product_id, product, price, unit, brand, item_type)
                VALUES
                  (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    provider,
                    branch,
                    ts,
                    product_id,
                    msg.get("product"),
                    msg.get("price"),
                    msg.get("unit"),
                    brand,
                    item_type,
                ),
            )
        else:  # promoFull
            # מיפוי: product (תיאור מבצע) -> promo_desc
            #        price -> discounted_price
            #        unit (MinQty מספרי) -> min_qty
            cur.execute(
                """
                INSERT INTO promos_full
                  (provider, branch, ts, product_id, promo_desc, discounted_price, min_qty, brand, item_type)
                VALUES
                  (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    provider,
                    branch,
                    ts,
                    product_id,
                    msg.get("product"),
                    msg.get("price"),
                    msg.get("unit"),
                    brand,
                    item_type,
                ),
            )
