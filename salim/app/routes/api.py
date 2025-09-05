import os
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any

router = APIRouter()

def connect_to_database():
    db_connection_string = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/salim_db")
    return psycopg2.connect(db_connection_string, cursor_factory=RealDictCursor)

@router.get("/products")
async def fetch_products_list(max_items: int = Query(100, description="How many products to show")):
    try:
        db_conn = connect_to_database()
        db_cursor = db_conn.cursor()
        
        db_cursor.execute("""
            SELECT p.barcode, p.canonical_name, p.price, p.promo_price, p.promo_text,
                   s.name as store_name, s.address, s.city
            FROM products p
            JOIN supermarkets s ON p.supermarket_id = s.supermarket_id
            ORDER BY p.collected_at DESC
            LIMIT %s
        """, (max_items,))
        
        product_rows = db_cursor.fetchall()
        db_cursor.close()
        db_conn.close()

        if not product_rows:
            raise HTTPException(status_code=404, detail="No products found")

        product_list = []
        for item in product_rows:
            product_data = {
                "barcode": item["barcode"],
                "name": item["canonical_name"],
                "price": float(item["price"]),
                "store": item["store_name"],
                "address": item["address"],
                "city": item["city"]
            }
            
            if item["promo_price"] and item["promo_price"] < item["price"]:
                product_data["promo_price"] = float(item["promo_price"])
                product_data["promo_text"] = item["promo_text"]
                original_price = item["price"]
                promo_price = item["promo_price"]
                product_data["discount"] = round(((original_price - promo_price) / original_price) * 100, 1)
            
            product_list.append(product_data)

        return {"products": product_list, "count": len(product_list)}
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))

@router.get("/product/by-barcode/{barcode}")
async def find_product_by_barcode(barcode: str):
    try:
        db_conn = connect_to_database()
        db_cursor = db_conn.cursor()
        
        db_cursor.execute("""
            SELECT p.barcode, p.canonical_name, p.price, p.promo_price, p.promo_text,
                   s.name as store_name, s.address, s.city
            FROM products p
            JOIN supermarkets s ON p.supermarket_id = s.supermarket_id
            WHERE p.barcode = %s
            ORDER BY p.collected_at DESC
        """, (barcode,))
        
        matching_products = db_cursor.fetchall()
        db_cursor.close()
        db_conn.close()

        if not matching_products:
            raise HTTPException(status_code=404, detail="Product not found")

        product_results = []
        for product_row in matching_products:
            product_info = {
                "barcode": product_row["barcode"],
                "name": product_row["canonical_name"],
                "price": float(product_row["price"]),
                "store": product_row["store_name"],
                "address": product_row["address"],
                "city": product_row["city"]
            }
            
            if product_row["promo_price"] and product_row["promo_price"] < product_row["price"]:
                product_info["promo_price"] = float(product_row["promo_price"])
                product_info["promo_text"] = product_row["promo_text"]
                regular_price = product_row["price"]
                sale_price = product_row["promo_price"]
                product_info["discount"] = round(((regular_price - sale_price) / regular_price) * 100, 1)
            
            product_results.append(product_info)

        return {"products": product_results}
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))

@router.get("/product/search")
async def search_products_by_name(search_query: str = Query(..., description="What product are you looking for?")):
    try:
        db_conn = connect_to_database()
        db_cursor = db_conn.cursor()
        
        db_cursor.execute("""
            SELECT p.barcode, p.canonical_name, p.price, p.promo_price, p.promo_text,
                   s.name as store_name, s.address, s.city
            FROM products p
            JOIN supermarkets s ON p.supermarket_id = s.supermarket_id
            WHERE p.canonical_name ILIKE %s
            ORDER BY p.collected_at DESC
            LIMIT 50
        """, (f"%{search_query}%",))
        
        search_results = db_cursor.fetchall()
        db_cursor.close()
        db_conn.close()

        if not search_results:
            raise HTTPException(status_code=404, detail="No products found")

        found_products = []
        for result_item in search_results:
            product_details = {
                "barcode": result_item["barcode"],
                "name": result_item["canonical_name"],
                "price": float(result_item["price"]),
                "store": result_item["store_name"],
                "address": result_item["address"],
                "city": result_item["city"]
            }
            
            if result_item["promo_price"] and result_item["promo_price"] < result_item["price"]:
                product_details["promo_price"] = float(result_item["promo_price"])
                product_details["promo_text"] = result_item["promo_text"]
                full_price = result_item["price"]
                discounted_price = result_item["promo_price"]
                product_details["discount"] = round(((full_price - discounted_price) / full_price) * 100, 1)
            
            found_products.append(product_details)

        return {"products": found_products}
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))

@router.get("/stores")
async def get_all_supermarkets():
    try:
        db_conn = connect_to_database()
        db_cursor = db_conn.cursor()
        
        db_cursor.execute("""
            SELECT supermarket_id, name, address, city
            FROM supermarkets
            ORDER BY name
        """)
        
        store_data = db_cursor.fetchall()
        db_cursor.close()
        db_conn.close()

        supermarket_list = []
        for store_info in store_data:
            supermarket_list.append({
                "id": store_info["supermarket_id"],
                "name": store_info["name"],
                "address": store_info["address"],
                "city": store_info["city"]
            })

        return {"stores": supermarket_list}
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))

@router.get("/health")
async def health():
    try:
        db_conn = connect_to_database()
        db_cursor = db_conn.cursor()
        db_cursor.execute("SELECT 1")
        db_cursor.close()
        db_conn.close()
        return {"status": "healthy"}
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))