from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Optional
import logging
from datetime import datetime, timedelta
from sqlalchemy import func, distinct

from ..database import get_db
from ..models import Product, Supermarket
from ..schemas import ProductResponse, PriceComparisonResponse, LowestPriceResponse, PriceHistoryResponse, PriceHistoryEntry

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

@router.get("/lowest-prices", 
            response_model=List[LowestPriceResponse],
            summary="Find lowest price products in each store",
            description="Find the cheapest version of products across all supermarkets, showing the best deals available")
def get_lowest_prices(
    db: Session = Depends(get_db),
    category: Optional[str] = Query(None, description="Filter by product category"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results per store")
):
    """Get the lowest priced products in each supermarket"""
    try:
        # Get lowest price products per supermarket
        subquery = db.query(
            Product.supermarket_id,
            func.min(func.coalesce(Product.promo_price, Product.price)).label('min_price')
        )
        
        if category:
            subquery = subquery.filter(Product.category == category)
            
        subquery = subquery.group_by(Product.supermarket_id).subquery()
        
        # Get the actual products with those lowest prices
        results = db.query(
            Product.product_id,
            Product.supermarket_id,
            Supermarket.name.label('supermarket_name'),
            Product.canonical_name,
            Product.brand,
            Product.category,
            Product.barcode,
            Product.price,
            Product.promo_price,
            func.coalesce(Product.promo_price, Product.price).label('effective_price')
        ).join(
            Supermarket, Product.supermarket_id == Supermarket.supermarket_id
        ).join(
            subquery, 
            (Product.supermarket_id == subquery.c.supermarket_id) & 
            (func.coalesce(Product.promo_price, Product.price) == subquery.c.min_price)
        )
        
        if category:
            results = results.filter(Product.category == category)
            
        results = results.order_by(func.coalesce(Product.promo_price, Product.price)).limit(limit * 3).all()
        
        if not results:
            return []
        
        # Calculate savings percent compared to highest price
        max_price = max(float(result.effective_price) for result in results)
        
        lowest_prices = []
        for result in results:
            effective_price = float(result.effective_price)
            savings_percent = ((max_price - effective_price) / max_price * 100) if max_price > 0 else 0
            
            lowest_price = LowestPriceResponse(
                product_id=result.product_id,
                supermarket_id=result.supermarket_id,
                supermarket_name=result.supermarket_name,
                canonical_name=result.canonical_name,
                brand=result.brand,
                category=result.category,
                barcode=result.barcode,
                price=result.price,
                promo_price=result.promo_price,
                effective_price=result.effective_price,
                savings_percent=savings_percent if savings_percent > 0 else None
            )
            lowest_prices.append(lowest_price)
        
        return lowest_prices
    except Exception as e:
        logger.error(f"Error fetching lowest prices: {e}")
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
            Product.product_id,
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
                product_id=result.product_id,
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

@router.get("/price-history/{barcode}", 
            response_model=PriceHistoryResponse,
            summary="Get price history for a product",
            description="Track price changes over time for a specific product across all supermarkets")
def get_price_history(
    barcode: str,
    db: Session = Depends(get_db),
    days: int = Query(30, ge=1, le=365, description="Number of days to look back for price history")
):
    """Get price history for a product by barcode"""
    try:
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Get price history
        results = db.query(
            Product.product_id,
            Product.collected_at,
            Product.price,
            Product.promo_price,
            func.coalesce(Product.promo_price, Product.price).label('effective_price'),
            Supermarket.name.label('supermarket_name'),
            Product.canonical_name,
            Product.brand,
            Product.category
        ).join(
            Supermarket, Product.supermarket_id == Supermarket.supermarket_id
        ).filter(
            Product.barcode == barcode,
            Product.collected_at >= start_date,
            Product.collected_at <= end_date
        ).order_by(Product.collected_at.desc()).all()
        
        if not results:
            raise HTTPException(status_code=404, detail="No price history found for this barcode")
        
        # Convert to response format
        price_history = []
        for result in results:
            entry = PriceHistoryEntry(
                product_id=result.product_id,
                date=result.collected_at,
                price=result.price,
                promo_price=result.promo_price,
                effective_price=result.effective_price,
                supermarket_name=result.supermarket_name
            )
            price_history.append(entry)
        
        # Get current price range
        current_prices = [float(entry.effective_price) for entry in price_history]
        current_lowest = min(current_prices)
        current_highest = max(current_prices)
        
        # Determine price trend (simple algorithm - compare first half with second half)
        mid_point = len(price_history) // 2
        if mid_point > 0:
            recent_avg = sum(float(entry.effective_price) for entry in price_history[:mid_point]) / mid_point
            older_avg = sum(float(entry.effective_price) for entry in price_history[mid_point:]) / (len(price_history) - mid_point)
            
            if recent_avg > older_avg * 1.05:  # 5% threshold
                trend = "increasing"
            elif recent_avg < older_avg * 0.95:  # 5% threshold
                trend = "decreasing"
            else:
                trend = "stable"
        else:
            trend = "stable"
        
        # Use first result for product info
        first_result = results[0]
        
        return PriceHistoryResponse(
            barcode=barcode,
            canonical_name=first_result.canonical_name,
            brand=first_result.brand,
            category=first_result.category,
            price_history=price_history,
            current_lowest_price=current_lowest,
            current_highest_price=current_highest,
            price_trend=trend
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching price history for barcode {barcode}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")