from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from typing import List, Optional
import logging

from ..database import get_db
from ..models import Supermarket, Product
from ..schemas import SupermarketResponse, ProductResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/supermarkets", tags=["supermarkets"])

@router.get("", response_model=List[SupermarketResponse])
def get_supermarkets(db: Session = Depends(get_db)):
    """Get all supermarkets"""
    try:
        supermarkets = db.query(Supermarket).all()
        return supermarkets
    except Exception as e:
        logger.error(f"Error fetching supermarkets: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/{supermarket_id}", response_model=SupermarketResponse)
def get_supermarket(supermarket_id: int, db: Session = Depends(get_db)):
    """Get a specific supermarket by ID"""
    try:
        supermarket = db.query(Supermarket).filter(Supermarket.supermarket_id == supermarket_id).first()
        if not supermarket:
            raise HTTPException(status_code=404, detail="Supermarket not found")
        return supermarket
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching supermarket {supermarket_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/{supermarket_id}/products", response_model=List[ProductResponse])
def get_supermarket_products(
    supermarket_id: int,
    db: Session = Depends(get_db),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    category: Optional[str] = None,
    search: Optional[str] = None
):
    """Get products from a specific supermarket"""
    try:
        query = db.query(Product).filter(Product.supermarket_id == supermarket_id)
        
        if category:
            query = query.filter(Product.category == category)
        
        if search:
            query = query.filter(Product.canonical_name.ilike(f"%{search}%"))
        
        products = query.offset(offset).limit(limit).all()
        return products
    except Exception as e:
        logger.error(f"Error fetching products for supermarket {supermarket_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")