import sqlite3

DB = r"data\prices.db"

con = sqlite3.connect(DB)
cur = con.cursor()
cur.execute("PRAGMA foreign_keys=ON")

cur.execute("""
CREATE TABLE IF NOT EXISTS enriched_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    product_raw TEXT NOT NULL,
    product_name TEXT NOT NULL,
    qty REAL,
    qty_unit TEXT,
    unit_std TEXT,
    price REAL NOT NULL,
    price_per_unit REAL,
    meta JSON
)
""")

cur.execute("CREATE INDEX IF NOT EXISTS ix_enriched_msg ON enriched_items(message_id)")
cur.execute("CREATE INDEX IF NOT EXISTS ix_enriched_name ON enriched_items(product_name)")
con.commit()
con.close()
print("enriched_items ready.")
