import sqlite3
from ingest_consumer.enricher import enrich_message

DB = r"data\prices.db"
con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row

rows = con.execute("""
    SELECT m.id
    FROM messages m
    LEFT JOIN enriched_items e ON e.message_id = m.id
    GROUP BY m.id
    HAVING COUNT(e.id) = 0
""").fetchall()

total = 0
for r in rows:
    cnt = enrich_message(con, r["id"])
    total += cnt
    print(f"[+] enriched message {r['id']} -> {cnt} rows")

con.close()
print(f"Done. inserted {total} rows.")
