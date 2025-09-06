from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import sqlite3

DB_PATH = "data/prices.db"

app = FastAPI(
    title="Groceries API",
    description="Supermarkets & Products endpoints (SQLite)",
    version="1.0.0",
)

def q(sql: str, params: tuple = ()):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        cur = con.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        return rows
    finally:
        con.close()

# ---------- Pydantic Models ----------

class SupermarketResponse(BaseModel):
    supermarket_id: int
    name: Optional[str] = None
    branch_name: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    created_at: Optional[str] = None

class ProductResponse(BaseModel):
    product_id: int
    supermarket_id: int
    barcode: Optional[str] = None
    canonical_name: str
    brand: Optional[str] = None
    category: Optional[str] = None
    size_value: Optional[float] = None
    size_unit: Optional[str] = None
    price: float
    currency: str
    promo_price: Optional[float] = None
    promo_text: Optional[str] = None
    in_stock: bool
    collected_at: str

class PriceComparisonResponse(BaseModel):
    product_id: int
    supermarket_id: int
    supermarket_name: Optional[str] = None
    canonical_name: str
    brand: Optional[str] = None
    category: Optional[str] = None
    barcode: Optional[str] = None
    price: float
    promo_price: Optional[float] = None
    promo_text: Optional[str] = None
    size_value: Optional[float] = None
    size_unit: Optional[str] = None
    in_stock: bool
    savings: float

# ---------- Supermarkets ----------

@app.get("/supermarkets", response_model=List[SupermarketResponse], tags=["supermarkets"])
def list_supermarkets():
    rows = q("""
        SELECT supermarket_id, name, branch_name, city, address, website, created_at
        FROM supermarkets
        ORDER BY supermarket_id
    """)
    return rows

@app.get("/supermarkets/{supermarket_id}", response_model=SupermarketResponse, tags=["supermarkets"])
def get_supermarket(supermarket_id: int):
    rows = q("""
        SELECT supermarket_id, name, branch_name, city, address, website, created_at
        FROM supermarkets
        WHERE supermarket_id = ?
        """, (supermarket_id,))
    if not rows:
        raise HTTPException(status_code=404, detail="Supermarket not found")
    return rows[0]

@app.get("/supermarkets/{supermarket_id}/products", response_model=List[ProductResponse], tags=["supermarkets"])
def products_by_supermarket(supermarket_id: int, search: Optional[str] = Query(default=None, description="search in name")):
    params = [supermarket_id]
    where = "WHERE supermarket_id = ?"
    if search:
        where += " AND canonical_name LIKE ?"
        params.append(f"%{search}%")
    rows = q(f"""
        SELECT
          product_id, supermarket_id, barcode, canonical_name, brand, category,
          size_value, size_unit, price, currency, promo_price, promo_text,
          in_stock, collected_at
        FROM products_vw
        {where}
        ORDER BY collected_at DESC, price ASC
        LIMIT 200
    """, tuple(params))
    return rows

# ---------- Products ----------

@app.get("/products", response_model=List[ProductResponse], tags=["products"])
def search_products(
    name: Optional[str] = Query(default=None, alias="q"),
    promo: Optional[bool] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    supermarket_id: Optional[int] = None,
    limit: int = 200,
    offset: int = 0,
):
    where = []
    params: list = []
    if name:
        where.append("canonical_name LIKE ?")
        params.append(f"%{name}%")
    if supermarket_id is not None:
        where.append("supermarket_id = ?")
        params.append(supermarket_id)
    if min_price is not None:
        where.append("price >= ?")
        params.append(min_price)
    if max_price is not None:
        where.append("price <= ?")
        params.append(max_price)
    if promo is True:
        where.append("promo_price IS NOT NULL")
    if promo is False:
        where.append("promo_price IS NULL")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    rows = q(f"""
        SELECT
          product_id, supermarket_id, barcode, canonical_name, brand, category,
          size_value, size_unit, price, currency, promo_price, promo_text,
          in_stock, collected_at
        FROM products_vw
        {where_sql}
        ORDER BY collected_at DESC, price ASC
        LIMIT ? OFFSET ?
    """, tuple(params + [limit, offset]))
    return rows

@app.get("/products/barcode/{barcode}", response_model=List[PriceComparisonResponse], tags=["products"])
def by_barcode(barcode: str):
    rows = q("""
        SELECT
          product_id,
          supermarket_id,
          (SELECT name FROM supermarkets s WHERE s.supermarket_id = pv.supermarket_id) AS supermarket_name,
          canonical_name, brand, category, barcode, price, promo_price, promo_text,
          size_value, size_unit, in_stock,
          0.0 AS savings
        FROM products_vw pv
        WHERE barcode = ?
        ORDER BY price ASC
        LIMIT 200
    """, (barcode,))
    if rows:
        min_price = rows[0]["price"]
        for r in rows:
            r["savings"] = round(r["price"] - min_price, 2)
    return rows
