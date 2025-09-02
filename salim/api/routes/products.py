from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Optional
import logging
from datetime import datetime, timedelta
from sqlalchemy import func

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
    name: Optional[str] = Query(None, description="Filter by product name (alias for 'q')"),
    min_price: Optional[float] = Query(None, description="Minimum price filter"),
    max_price: Optional[float] = Query(None, description="Maximum price filter"),
    supermarket_id: Optional[int] = Query(None, description="Filter by specific supermarket ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip")
):
    """Search products with various filters"""
    try:
        query = db.query(Product)
        search_term = name
        if search_term:
            query = query.filter(Product.name.ilike(f"%{search_term}%"))
        if supermarket_id is not None:
            query = query.filter(Product.branch_number == supermarket_id)
        if min_price is not None:
            query = query.filter(Product.price >= min_price)
        if max_price is not None:
            query = query.filter(Product.price <= max_price)
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
    name: Optional[str] = Query(None, description="Filter by product name"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results per store")
):
    """Get the lowest priced products in each supermarket"""
    try:
        subquery = db.query(
            Product.super_id,
            func.min(Product.price).label('min_price')
        )
        if name:
            subquery = subquery.filter(Product.name.ilike(f"%{name}%"))
        subquery = subquery.group_by(Product.super_id).subquery()

        query = db.query(
            Product.id.label('product_id'),
            Product.super_id,
            Supermarket.branch_name.label('supermarket_name'),
            Product.name,
            Product.barcode,
            Product.price
        ).join(
            Supermarket, Product.super_id == Supermarket.id
        ).join(
            subquery,
            (Product.super_id == subquery.c.super_id) &
            (Product.price == subquery.c.min_price)
        )
        if name:
            query = query.filter(Product.name.ilike(f"%{name}%"))
        results = query.limit(limit).all()

        prices = [float(r.price) for r in results]
        max_price = max(prices) if prices else 0

        lowest_prices = []
        for r in results:
            savings_percent = ((max_price - float(r.price)) / max_price * 100) if max_price > 0 else None
            lowest_price = LowestPriceResponse(
                product_id=str(r.product_id),
                supermarket_id=str(r.super_id),
                supermarket_name=r.supermarket_name,
                name=r.name,
                barcode=r.barcode,
                price=r.price,
                effective_price=r.price,
                savings_percent=savings_percent if savings_percent and savings_percent > 0 else None
            )
            lowest_prices.append(lowest_price)

        return lowest_prices
    except Exception as e:
        logger.error(f"Error fetching lowest prices: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/{product_id}", response_model=ProductResponse)
def get_product(product_id: str, db: Session = Depends(get_db)):
    """Get a specific product by ID"""
    try:
        product = db.query(Product).filter(Product.id == product_id).first()
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
        results = db.query(
            Product.id.label('product_id'),
            Product.super_id,
            Supermarket.branch_name.label('branch_name'),
            Product.name,
            Product.barcode,
            Product.price,
        ).join(
            Supermarket, Product.super_id == Supermarket.id
        ).filter(
            Product.barcode == barcode
        ).all()

        if not results:
            raise HTTPException(status_code=404, detail="No products found with this barcode")

        comparisons = []
        for result in results:
            comparison = PriceComparisonResponse(
                product_id=str(result.product_id),
                supermarket_id=str(result.super_id),
                supermarket_name=result.branch_name,
                name=result.name,
                barcode=result.barcode,
                price=result.price,
            )
            comparisons.append(comparison)

        comparisons.sort(key=lambda x: x.price if x.price is not None else float('inf'))
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
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        results = db.query(
            Product.id.label('product_id'),
            Product.updated_at,
            Product.price,
            Supermarket.branch_name.label('supermarket_name'),
            Product.name,
            Product.barcode
        ).join(
            Supermarket, Product.super_id == Supermarket.id
        ).filter(
            Product.barcode == barcode,
            Product.updated_at >= start_date,
            Product.updated_at <= end_date
        ).order_by(Product.updated_at.desc()).all()

        if not results:
            raise HTTPException(status_code=404, detail="No price history found for this barcode")

        price_history = []
        for result in results:
            entry = PriceHistoryEntry(
                product_id=str(result.product_id),
                date=result.updated_at,
                price=result.price,
                promo_price=None,
                effective_price=result.price,
                supermarket_name=result.supermarket_name
            )
            price_history.append(entry)

        current_prices = [float(entry.effective_price) for entry in price_history]
        current_lowest = min(current_prices)
        current_highest = max(current_prices)

        mid_point = len(price_history) // 2
        if mid_point > 0:
            recent_avg = sum(float(entry.effective_price) for entry in price_history[:mid_point]) / mid_point
            older_avg = sum(float(entry.effective_price) for entry in price_history[mid_point:]) / (len(price_history) - mid_point)
            if recent_avg > older_avg * 1.05:
                trend = "increasing"
            elif recent_avg < older_avg * 0.95:
                trend = "decreasing"
            else:
                trend = "stable"
        else:
            trend = "stable"

        first_result = results[0]

        return PriceHistoryResponse(
            barcode=barcode,
            canonical_name=first_result.name,
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