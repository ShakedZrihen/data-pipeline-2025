from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime

from models import ProductResponse
from database import db_service
from routes.api.supermarkets import get_supermarket

router = APIRouter(prefix="/products", tags=["products"])


@router.get("/", response_model=List[ProductResponse])
async def search_products(
    name: Optional[str] = Query(None, description="Filter by product name (alias for 'q')"),
    promo: Optional[bool] = Query(None, description="Filter by promotion status"),
    min_price: Optional[float] = Query(None, description="Minimum price filter"),
    max_price: Optional[float] = Query(None, description="Maximum price filter"),
    supermarket_id: Optional[int] = Query(None, description="Filter by specific supermarket ID")
):
    """Search products with various filters"""
    try:
        provider = None
        branch = None
        
        # Handle supermarket_id filter
        if supermarket_id is not None:
            supermarket = await get_supermarket(supermarket_id)
            provider = supermarket.name
            branch = supermarket.branch_name
        
        # Search products with filters
        data = db_service.search_products(
            name=name,
            promo=promo,
            min_price=min_price,
            max_price=max_price,
            provider=provider,
            branch=branch
        )
        
        result = []
        for item in data:
            result.append(ProductResponse(
                id=item['id'],
                provider=item['provider'],
                branch=item['branch'],
                product_name=item['product_name'],
                manufacturer=item.get('manufacturer'),
                price=float(item['price']),
                unit=item['unit'],
                category=item.get('category'),
                is_promotion=item['is_promotion'],
                is_kosher=item.get('is_kosher'),
                file_timestamp=datetime.fromisoformat(item['file_timestamp'].replace('Z', '+00:00')),
                created_at=datetime.fromisoformat(item['created_at'].replace('Z', '+00:00'))
            ))
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error searching products: {e}")
        raise HTTPException(status_code=500, detail="Failed to search products")