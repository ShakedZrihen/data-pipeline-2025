import sqlite3

DB = r"data\prices.db"

def table_columns(cur, table):
    cur.execute(f"PRAGMA table_info('{table}')")
    return [r[1] for r in cur.fetchall()]

def main():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys=ON")

    cols = table_columns(cur, "messages")
    if "timestamp" in cols:
        ts_col = "timestamp"
    elif "ts" in cols:
        ts_col = "ts"
    elif "ts_iso" in cols:
        ts_col = "ts_iso"
    else:
        raise SystemExit(f"[!] לא נמצאה עמודת זמן ב-messages (columns: {cols})")

    print(f"[*] using time column: {ts_col}")

    cur.execute(f"""
        WITH keep AS (
          SELECT MIN(id) AS keep_id
          FROM messages
          GROUP BY provider, branch, type, {ts_col}
        )
        DELETE FROM messages
        WHERE id NOT IN (SELECT keep_id FROM keep)
    """)
    con.commit()

    cur.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_messages_pbt
        ON messages(provider, branch, type, {ts_col})
    """)
    con.commit()

    cur.execute("PRAGMA index_list('messages')")
    print("Indexes on 'messages':", cur.fetchall())
    cur.execute("SELECT COUNT(*) FROM messages")
    print("messages rows:", cur.fetchone()[0])

    con.close()
    print("Done.")

if __name__ == "__main__":
    main()
