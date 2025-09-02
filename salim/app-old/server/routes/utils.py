from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
import logging

from ..database import get_db
from ..models import Product, Supermarket

logger = logging.getLogger(__name__)

router = APIRouter(tags=["utilities"])

@router.get("/categories")
def get_categories(db: Session = Depends(get_db)):
    """Get all available product categories"""
    try:
        from sqlalchemy import func
        categories = db.query(Product.category).distinct().order_by(Product.category).all()
        return [cat[0] for cat in categories if cat[0]]
    except Exception as e:
        logger.error(f"Error fetching categories: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/brands")
def get_brands(db: Session = Depends(get_db)):
    """Get all available brands"""
    try:
        from sqlalchemy import func
        brands = db.query(Product.brand).distinct().order_by(Product.brand).all()
        return [brand[0] for brand in brands if brand[0]]
    except Exception as e:
        logger.error(f"Error fetching brands: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    """Get database statistics"""
    try:
        from sqlalchemy import func
        
        total_supermarkets = db.query(func.count(Supermarket.supermarket_id)).scalar()
        total_products = db.query(func.count(Product.product_id)).scalar()
        products_on_sale = db.query(func.count(Product.product_id)).filter(Product.promo_price.isnot(None)).scalar()
        avg_price = db.query(func.avg(Product.price)).scalar()
        
        return {
            "total_supermarkets": total_supermarkets,
            "total_products": total_products,
            "products_on_sale": products_on_sale,
            "average_price": round(float(avg_price), 2) if avg_price else 0,
            "sale_percentage": round((products_on_sale / total_products) * 100, 1) if total_products > 0 else 0
        }
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "salim-api"}