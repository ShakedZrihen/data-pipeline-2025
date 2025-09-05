from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional, Any
from .db import get_conn

app = FastAPI(title="Prices API", version="0.1")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/providers", response_model=List[str])
def list_providers():
    with get_conn() as con:
        cur = con.execute("SELECT DISTINCT provider FROM messages ORDER BY provider")
        return [r["provider"] for r in cur.fetchall()]

@app.get("/branches/{provider}", response_model=List[str])
def list_branches(provider: str):
    with get_conn() as con:
        cur = con.execute(
            "SELECT DISTINCT branch FROM messages WHERE provider=? ORDER BY branch",
            (provider,),
        )
        rows = [r["branch"] for r in cur.fetchall()]
        if not rows:
            raise HTTPException(404, detail="provider not found")
        return rows

@app.get("/latest/{provider}/{branch}")
def latest_dump(provider: str, branch: str, limit: int = 50):
    with get_conn() as con:
        m = con.execute(
            "SELECT id, ts_iso, items_total FROM messages "
            "WHERE provider=? AND branch=? "
            "ORDER BY ts_iso DESC, id DESC LIMIT 1",
            (provider, branch),
        ).fetchone()
        if not m:
            raise HTTPException(404, detail="no data for provider/branch")

        items = con.execute(
            "SELECT product, price, unit FROM items WHERE message_id=? LIMIT ?",
            (m["id"], limit),
        ).fetchall()

        return {
            "provider": provider,
            "branch": branch,
            "timestamp": m["ts_iso"],
            "items_total": m["items_total"],
            "items": [dict(r) for r in items],
        }

@app.get("/search")
def search_products(q: str = Query(..., min_length=2), limit: int = 20):

    like = f"%{q}%"
    with get_conn() as con:
        rows = con.execute(
            "SELECT i.product, i.price, i.unit, m.provider, m.branch, m.ts_iso "
            "FROM items i JOIN messages m ON m.id = i.message_id "
            "WHERE i.product LIKE ? "
            "ORDER BY m.ts_iso DESC, m.id DESC LIMIT ?",
            (like, limit),
        ).fetchall()
        return [dict(r) for r in rows]
