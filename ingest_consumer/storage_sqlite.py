import sqlite3
from pathlib import Path

DDL = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT NOT NULL,
  branch   TEXT NOT NULL,
  type     TEXT NOT NULL,
  ts_iso   TEXT NOT NULL,
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

def save_message(db_path: str, msg: dict):
    conn = _connect(db_path)
    try:
        conn.executescript(DDL)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO messages(provider,branch,type,ts_iso,items_total) VALUES (?,?,?,?,?)",
            (msg["provider"], msg["branch"], msg["type"], msg["timestamp"], msg["items_total"]),
        )
        mid = cur.lastrowid
        for it in msg.get("items_sample", []) or []:
            cur.execute(
                "INSERT INTO items(message_id,product,price,unit) VALUES (?,?,?,?)",
                (mid, it["product"], it["price"], it.get("unit")),
            )
        conn.commit()
    finally:
        conn.close()
