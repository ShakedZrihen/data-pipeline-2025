import os, json, sqlite3
from pathlib import Path

DDL = """
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider   TEXT NOT NULL,
  branch     TEXT NOT NULL,
  type       TEXT NOT NULL,
  ts_iso     TEXT NOT NULL,
  items_total INTEGER NOT NULL DEFAULT 0,
  raw_json   TEXT,
  s3_bucket  TEXT,
  s3_key     TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_messages_unique
ON messages(provider, branch, type, ts_iso);

CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts_iso);
CREATE INDEX IF NOT EXISTS idx_messages_provider_branch_ts
ON messages(provider, branch, ts_iso);

CREATE TABLE IF NOT EXISTS items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  product TEXT NOT NULL,
  price   REAL,
  unit    TEXT
);
CREATE INDEX IF NOT EXISTS idx_items_message_id ON items(message_id);

CREATE TABLE IF NOT EXISTS supermarkets (
  provider TEXT NOT NULL,
  branch   TEXT NOT NULL,
  name_hint TEXT,
  PRIMARY KEY(provider, branch)
);
"""

def _connect(db_path: str) -> sqlite3.Connection:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(p.as_posix())
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def get_db_path() -> str:
    return os.environ.get("DB_PATH", os.path.join("data", "prices.db"))

def init_db(db_path: str) -> None:
    with _connect(db_path) as con:
        con.executescript(DDL)

def upsert_supermarket(db_path: str, provider: str, branch: str, name: str | None = None) -> None:
    with _connect(db_path) as con:
        con.execute("""
            INSERT INTO supermarkets(provider, branch, name_hint)
            VALUES(?, ?, ?)
            ON CONFLICT(provider, branch) DO UPDATE SET
              name_hint = COALESCE(excluded.name_hint, supermarkets.name_hint)
        """, (provider, branch, name or provider))
        con.commit()

def save_message(db_path: str, msg: dict) -> int | None:

    items = (msg.get("items") or msg.get("items_sample") or [])[:]
    with _connect(db_path) as con:
        cur = con.cursor()
        cur.executescript(DDL)

        cur.execute("""
            INSERT OR IGNORE INTO messages(provider, branch, type, ts_iso, items_total, raw_json, s3_bucket, s3_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(msg["provider"]),
            str(msg["branch"]),
            str(msg["type"]),
            str(msg.get("ts_iso") or msg.get("timestamp")),
            int(msg.get("items_total") or len(items) or 0),
            json.dumps(msg, ensure_ascii=False),
            msg.get("s3_bucket"),
            msg.get("s3_key"),
        ))
        con.commit()

        if cur.lastrowid:
            message_id = cur.lastrowid
        else:
            cur.execute("""
                SELECT id FROM messages
                WHERE provider=? AND branch=? AND type=? AND ts_iso=?
                LIMIT 1
            """, (
                str(msg["provider"]), str(msg["branch"]), str(msg["type"]),
                str(msg.get("ts_iso") or msg.get("timestamp")),
            ))
            row = cur.fetchone()
            message_id = row[0] if row else None

        if message_id is None:
            return None

        cur.execute("UPDATE messages SET items_total=? WHERE id=?",
                    (int(msg.get("items_total") or len(items) or 0), message_id))

        cur.execute("DELETE FROM items WHERE message_id=?", (message_id,))
        if items:
            cur.executemany("""
                INSERT INTO items(message_id, product, price, unit)
                VALUES (?, ?, ?, ?)
            """, [
                (message_id,
                 str(it.get("product") or "").strip(),
                 float(it["price"]) if it.get("price") is not None else None,
                 it.get("unit"))
                for it in items
            ])
        con.commit()
        return message_id
