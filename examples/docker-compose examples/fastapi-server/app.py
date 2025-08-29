from fastapi import FastAPI, HTTPException, Query
import psycopg2
import os
from typing import List, Dict, Any, Optional
import json
from psycopg2.extras import RealDictCursor

app = FastAPI(title="Products API", description="API for products and stores")

def get_db_connection():
    """Create PostgreSQL connection"""
    try:
        connection = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            database=os.getenv('POSTGRES_DB', 'pricedb'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
            port=5432,
            cursor_factory=RealDictCursor
        )
        return connection
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.get("/products")
async def get_products(limit: int = Query(100, ge=1, le=1000)):
    """Get all available products with their available stores"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = """
        SELECT 
            p.*,
            COALESCE(
                json_agg(
                    json_build_object(
                        'store_id', s.store_id,
                        'store_name', s.store_name,
                        'store_type', s.store_type,
                        'city', s.city
                    )
                    ORDER BY s.store_name
                ) FILTER (WHERE s.id IS NOT NULL), 
                '[]'::json
            ) as available_in_supers
        FROM price_items p
        LEFT JOIN product_store_availability psa ON p.id = psa.price_item_id
        LEFT JOIN stores s ON psa.store_id = s.id
        GROUP BY p.id
        ORDER BY p.created_at DESC
        LIMIT %s
        """
        cursor.execute(query, (limit,))
        results = cursor.fetchall()
        
        products = []
        for row in results:
            product = dict(row)
            products.append({
                "product": {
                    **{k: v for k, v in product.items() if k != 'available_in_supers'},
                    "availableInSupers": product['available_in_supers']
                }
            })
        
        return products
    finally:
        conn.close()

@app.get("/supers")
async def get_supers():
    """Get all available stores"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM stores ORDER BY store_name")
        stores = cursor.fetchall()
        return [dict(store) for store in stores]
    finally:
        conn.close()

@app.get("/products/{barcode}")
async def get_product_by_barcode(barcode: str):
    """Get one product by barcode with available stores"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = """
        SELECT 
            p.*,
            COALESCE(
                json_agg(
                    json_build_object(
                        'store_id', s.store_id,
                        'store_name', s.store_name,
                        'store_type', s.store_type,
                        'city', s.city
                    )
                    ORDER BY s.store_name
                ) FILTER (WHERE s.id IS NOT NULL), 
                '[]'::json
            ) as available_in_supers
        FROM price_items p
        LEFT JOIN product_store_availability psa ON p.id = psa.price_item_id
        LEFT JOIN stores s ON psa.store_id = s.id
        WHERE p.item_code = %s
        GROUP BY p.id
        """
        cursor.execute(query, (barcode,))
        result = cursor.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Product not found")
        
        product = dict(result)
        return {
            "product": {
                **{k: v for k, v in product.items() if k != 'available_in_supers'},
                "availableInSupers": product['available_in_supers']
            }
        }
    finally:
        conn.close()

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Products API",
        "endpoints": {
            "products": "/products?limit=100",
            "supers": "/supers", 
            "product_by_barcode": "/products/{barcode}"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)