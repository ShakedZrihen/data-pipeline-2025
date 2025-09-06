from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
import logging

from ..database import get_db
from ..models import Product, Supermarket 
from sqlalchemy import func, String

logger = logging.getLogger(__name__)

router = APIRouter(tags=["utilities"])

@router.get("/stats")
def get_stats(
    db: Session = Depends(get_db),
    product_name: str = None,
    provider: str = None
):
    """Get database statistics, with optional filters for product name and provider"""
    try:
        product_filters = []
        supermarket_filters = []

        if product_name:
            product_filters.append(Product.name.ilike(f"%{product_name}%"))
        if provider:
            supermarket_filters.append(Supermarket.provider.ilike(f"%{provider}%"))

        total_supermarkets = db.query(func.count(Supermarket.id)).filter(*supermarket_filters).scalar()

        if supermarket_filters:
            total_products_query = db.query(Product).join(Supermarket, Product.super_id == Supermarket.id).filter(*product_filters, *supermarket_filters)
            avg_price_query = db.query(func.avg(Product.price)).join(Supermarket, Product.super_id == Supermarket.id).filter(*product_filters, *supermarket_filters)
        else:
            total_products_query = db.query(Product).filter(*product_filters)
            avg_price_query = db.query(func.avg(Product.price)).filter(*product_filters)

        total_products = total_products_query.count()
        avg_price = avg_price_query.scalar()

        result = {
            "total_supermarkets": total_supermarkets,
            "total_products": total_products,
            "average_price": round(float(avg_price), 2) if avg_price else 0,
        }

        return result
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "salim-api"}
