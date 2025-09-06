import sqlite3
from pathlib import Path
from typing import Dict, Any
import  json

DDL = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT NOT NULL,
  branch   TEXT NOT NULL,
  type     TEXT NOT NULL,
  ts_iso   TEXT NOT NULL,
  raw_json TEXT,
  items_total INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  product TEXT NOT NULL,
  price   REAL NOT NULL,
  unit    TEXT
);
"""

def _connect(db_path: str):
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(p.as_posix())
    return conn

def save_message(db_path: str, msg: Dict[str, Any]) -> int:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys=ON")
    con.executescript(DDL)

    cur.execute("""
        INSERT OR IGNORE INTO messages(provider, branch, type, ts_iso,raw_json, items_total)
        VALUES (?, ?, ?, ?,?, ?)
    """, (msg["provider"], msg["branch"], msg["type"], msg["timestamp"],json.dumps(msg, ensure_ascii=False), msg.get("items_total") or len(msg.get("items", []))))
    con.commit()

    if cur.lastrowid:
        message_id = cur.lastrowid
    else:
        cur.execute("""
            SELECT id FROM messages
            WHERE provider=? AND branch=? AND type=? AND ts_iso=?
            LIMIT 1
        """, (msg["provider"], msg["branch"], msg["type"], msg["timestamp"]))
        row = cur.fetchone()
        message_id = row[0] if row else None

    items = msg.get("items") or msg.get("items_sample") or []
    if items and message_id is not None:
        cur.execute("DELETE FROM items WHERE message_id = ?", (message_id,))
        cur.executemany("""
            INSERT INTO items(message_id, product, price, unit)
            VALUES (?, ?, ?, ?)
        """, [(message_id, it.get("product"), it.get("price"), it.get("unit")) for it in items])
        con.commit()

    con.close()
    return message_id