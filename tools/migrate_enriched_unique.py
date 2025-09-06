import sqlite3, os

db = os.environ.get("INGEST_SQLITE_PATH", r"data\prices.db")
con = sqlite3.connect(db)
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

cur.execute("""
DELETE FROM enriched_items
WHERE id NOT IN (
  SELECT MIN(id) FROM enriched_items
  GROUP BY message_id, product_name, unit_std
)
""")

cur.execute("DROP INDEX IF EXISTS ux_enriched_msg_prod")
cur.execute("""
CREATE UNIQUE INDEX IF NOT EXISTS ux_enriched_msg_prod_v2
ON enriched_items(message_id, product_name, unit_std)
""")

con.commit()
con.close()
print("OK: deduped and unique index is now on (message_id, product_name, unit_std)")
