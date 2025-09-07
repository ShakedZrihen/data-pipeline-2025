from fastapi import APIRouter, Depends, Query, HTTPException
from ..db import get_conn, fetch_all, fetch_one
from ..schemas import Supermarket, Product
import sqlite3

router = APIRouter(prefix="/supermarkets", tags=["supermarkets"])

def _conn():
    try:
        conn = get_conn()
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass

@router.get("", response_model=list[Supermarket])
def list_supermarkets(conn: sqlite3.Connection = Depends(_conn)):
    sql = """
    SELECT s.provider,
           s.branch,
           COALESCE(s.name_hint, s.provider) AS name,
           (SELECT COUNT(1) FROM messages m WHERE m.provider=s.provider AND m.branch=s.branch) AS items_messages
    FROM supermarkets s
    ORDER BY s.provider, CAST(s.branch AS TEXT)
    """
    return fetch_all(conn, sql)

@router.get("/{provider}/{branch}", response_model=Supermarket)
def get_supermarket(provider: str, branch: str, conn: sqlite3.Connection = Depends(_conn)):
    sql = """
    SELECT s.provider,
           s.branch,
           COALESCE(s.name_hint, s.provider) AS name,
           (SELECT COUNT(1) FROM messages m WHERE m.provider=s.provider AND m.branch=s.branch) AS items_messages
    FROM supermarkets s
    WHERE s.provider=? AND s.branch=?
    """
    row = fetch_one(conn, sql, (provider, branch))
    if not row:
        raise HTTPException(status_code=404, detail="Supermarket not found")
    return row

@router.get("/{provider}/{branch}/products", response_model=list[Product])
def supermarket_products(
    provider: str,
    branch: str,
    q: str | None = Query(default=None, description="חיפוש בשם המוצר"),
    limit: int = Query(50, ge=1, le=200),
    conn: sqlite3.Connection = Depends(_conn),
):
    last_sql = """
    SELECT id, ts_iso FROM messages
    WHERE provider=? AND branch=?
    ORDER BY ts_iso DESC, id DESC
    LIMIT 1
    """
    last = fetch_one(conn, last_sql, (provider, branch))
    if not last:
        return []
    params = [last["id"]]
    where = "WHERE i.message_id=?"
    if q:
        where += " AND i.product LIKE ?"
        params.append(f"%{q}%")
    sql = f"""
    SELECT i.message_id, i.product, i.price, i.unit,
           m.provider, m.branch, m.ts_iso
    FROM items i
    JOIN messages m ON m.id = i.message_id
    {where}
    ORDER BY i.product
    LIMIT ?
    """
    params.append(limit)
    return fetch_all(conn, sql, tuple(params))
