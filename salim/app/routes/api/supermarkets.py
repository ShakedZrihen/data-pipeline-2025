from fastapi import APIRouter, HTTPException, Query, Path
from typing import List, Optional
from datetime import datetime

from models import SupermarketResponse, ProductResponse
from database import db_service

router = APIRouter(prefix="/supermarkets", tags=["supermarkets"])


@router.get("/", response_model=List[SupermarketResponse])
async def get_supermarkets():
    """Get all supermarkets"""
    try:
        data = db_service.get_supermarkets()
        
        result = []
        for i, item in enumerate(data, 1):
            result.append(SupermarketResponse(
                supermarket_id=i,
                name=item['provider'],
                branch_name=item['branch'],
                city=None,
                address=None,
                website=None,
                created_at=datetime.fromisoformat(item['created_at'].replace('Z', '+00:00'))
            ))
        
        return result
        
    except Exception as e:
        print(f"Error fetching supermarkets: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch supermarkets")


@router.get("/{supermarket_id}", response_model=SupermarketResponse)
async def get_supermarket(supermarket_id: int = Path(..., description="The supermarket ID")):
    """Get a specific supermarket by ID"""
    try:
        supermarkets = await get_supermarkets()
        
        if supermarket_id < 1 or supermarket_id > len(supermarkets):
            raise HTTPException(status_code=404, detail="Supermarket not found")
        
        return supermarkets[supermarket_id - 1]
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error fetching supermarket {supermarket_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch supermarket")


@router.get("/{supermarket_id}/products", response_model=List[ProductResponse])
async def get_supermarket_products(
    supermarket_id: int = Path(..., description="The supermarket ID"),
    search: Optional[str] = Query(None, description="Search in product names")
):
    """Get products from a specific supermarket"""
    try:
        # First get the supermarket to validate ID and get provider/branch
        supermarket = await get_supermarket(supermarket_id)
        
        # Get products from this supermarket
        data = db_service.get_supermarket_products(
            provider=supermarket.name,
            branch=supermarket.branch_name,
            search=search
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
        print(f"Error fetching supermarket products: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch products")