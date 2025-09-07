import sqlite3
from pathlib import Path
from typing import Iterable, Any, Dict, Tuple

def get_conn(db_path: str = "data/prices.db") -> sqlite3.Connection:
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(p.as_posix())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def fetch_all(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> Iterable[Dict]:
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    return [dict(r) for r in rows]

def fetch_one(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> Dict | None:
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    return dict(row) if row else None
