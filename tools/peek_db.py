import sqlite3, os, json, sys
db = r"data\prices.db"
assert os.path.exists(db), f"DB not found: {db}"
con = sqlite3.connect(db)
cur = con.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cur.fetchall()]
print("Tables:", tables)

for t in ["messages","items"]:
    if t in tables:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        cnt = cur.fetchone()[0]
        print(f"\n{t}: {cnt} rows")
        cur.execute(f"SELECT * FROM {t} LIMIT 5")
        for row in cur.fetchall():
            print(row)

con.close()
