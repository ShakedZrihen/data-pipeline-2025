from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Optional
import logging

from ..database import get_db
from ..models import Product, Supermarket
from ..schemas import ProductResponse, PriceComparisonResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/products", tags=["products"])

@router.get("", 
            response_model=List[ProductResponse],
            summary="Search products",
            description="Search for products using various filters like name, category, brand, price range, and promotions")
def search_products(
    db: Session = Depends(get_db),
    q: Optional[str] = Query(None, description="Search query for product name"),
    name: Optional[str] = Query(None, description="Filter by product name (alias for 'q')"),
    category: Optional[str] = Query(None, description="Filter by product category"),
    brand: Optional[str] = Query(None, description="Filter by brand name"),
    promo: Optional[bool] = Query(None, description="Filter by promotion status (true=on sale, false=regular price)"),
    min_price: Optional[float] = Query(None, description="Minimum price filter"),
    max_price: Optional[float] = Query(None, description="Maximum price filter"),
    supermarket_id: Optional[int] = Query(None, description="Filter by specific supermarket ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip")
):
    """Search products with various filters"""
    try:
        query = db.query(Product)
        
        # Handle search by name (both 'q' and 'name' parameters work)
        search_term = q or name
        if search_term:
            query = query.filter(Product.canonical_name.ilike(f"%{search_term}%"))
        
        if category:
            query = query.filter(Product.category == category)
            
        if brand:
            query = query.filter(Product.brand.ilike(f"%{brand}%"))
            
        # Promo filter
        if promo is not None:
            if promo:
                query = query.filter(Product.promo_price.isnot(None))
            else:
                query = query.filter(Product.promo_price.is_(None))
            
        if min_price is not None:
            query = query.filter(Product.price >= min_price)
            
        if max_price is not None:
            query = query.filter(Product.price <= max_price)
            
        if supermarket_id:
            query = query.filter(Product.supermarket_id == supermarket_id)
        
        products = query.offset(offset).limit(limit).all()
        return products
    except Exception as e:
        logger.error(f"Error searching products: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/{product_id}", response_model=ProductResponse)
def get_product(product_id: int, db: Session = Depends(get_db)):
    """Get a specific product by ID"""
    try:
        product = db.query(Product).filter(Product.product_id == product_id).first()
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        return product
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching product {product_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/barcode/{barcode}", 
            response_model=List[PriceComparisonResponse],
            summary="Compare prices by barcode",
            description="Get all products with the same barcode across different supermarkets, sorted by price (cheapest first)")
def get_products_by_barcode(
    barcode: str,
    db: Session = Depends(get_db)
):
    """Get all products with the same barcode across different supermarkets with price comparison"""
    try:
        from sqlalchemy import func
        
        results = db.query(
            Product.supermarket_id,
            Supermarket.name.label('supermarket_name'),
            Product.canonical_name,
            Product.brand,
            Product.category,
            Product.barcode,
            Product.price,
            Product.promo_price,
            Product.promo_text,
            Product.size_value,
            Product.size_unit,
            Product.in_stock
        ).join(
            Supermarket, Product.supermarket_id == Supermarket.supermarket_id
        ).filter(
            Product.barcode == barcode
        ).all()
        
        if not results:
            raise HTTPException(status_code=404, detail="No products found with this barcode")
        
        # Convert to response format with price comparison
        comparisons = []
        for result in results:
            comparison = PriceComparisonResponse(
                supermarket_id=result.supermarket_id,
                supermarket_name=result.supermarket_name,
                canonical_name=result.canonical_name,
                brand=result.brand,
                category=result.category,
                barcode=result.barcode,
                price=result.price,
                promo_price=result.promo_price,
                promo_text=result.promo_text,
                size_value=result.size_value,
                size_unit=result.size_unit,
                in_stock=result.in_stock,
                savings=result.price - result.promo_price if result.promo_price else None
            )
            comparisons.append(comparison)
        
        # Sort by effective price (cheapest first)
        comparisons.sort(key=lambda x: x.promo_price if x.promo_price else x.price)
        
        return comparisons
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching products by barcode {barcode}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")