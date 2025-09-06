"""
Products API endpoints.
Provides endpoints for searching products and comparing prices across supermarkets.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
from typing import List, Optional
from decimal import Decimal

from ...database import get_db
from ...models import Product, Branch, Price
from ...schemas import ProductResponse, PriceComparisonResponse

router = APIRouter(prefix="/products", tags=["products"])


@router.get("/", response_model=List[ProductResponse])
async def search_products(
    name: Optional[str] = Query(None, description="Search in product name"),
    brand: Optional[str] = Query(None, description="Filter by brand"),
    min_price: Optional[float] = Query(None, ge=0, description="Minimum price"),
    max_price: Optional[float] = Query(None, ge=0, description="Maximum price"),
    provider: Optional[str] = Query(None, description="Filter by supermarket chain"),
    has_discount: Optional[bool] = Query(None, description="Only products with discounts"),
    limit: int = Query(100, ge=1, le=500, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: Session = Depends(get_db)
) -> List[ProductResponse]:
    """
    Search products with various filters.
    Returns products with their latest price information.
    
    Args:
        name: Search term for product name (supports Hebrew)
        brand: Filter by brand name
        min_price: Minimum price filter
        max_price: Maximum price filter
        provider: Filter by specific supermarket chain
        has_discount: If true, only return products with discount prices
        limit: Maximum number of results
        offset: Offset for pagination
    """
    try:
        
        latest_price_subquery = db.query(
            Price.product_id,
            func.max(Price.ts).label("latest_ts")
        ).group_by(Price.product_id).subquery()
        
       
        query = db.query(
            Product,
            Price.price,
            Price.discount_price,
            Price.final_price,
            Price.ts
        ).outerjoin(
            latest_price_subquery,
            Product.product_id == latest_price_subquery.c.product_id
        ).outerjoin(
            Price,
            and_(
                Product.product_id == Price.product_id,
                Price.ts == latest_price_subquery.c.latest_ts
            )
        )
        
        
        if name:
            query = query.filter(Product.product_name.ilike(f"%{name}%"))
        
        if brand:
            query = query.filter(Product.brand_name.ilike(f"%{brand}%"))
        
        
        if min_price is not None or max_price is not None or has_discount is not None:
            query = query.filter(Price.price_id.isnot(None))  
            
            if min_price is not None:
                query = query.filter(Price.final_price >= min_price)
            
            if max_price is not None:
                query = query.filter(Price.final_price <= max_price)
            
            if has_discount:
                query = query.filter(Price.discount_price.isnot(None))
        
        
        if provider:
            query = query.join(Branch).filter(Branch.provider == provider)
        
       
        query = query.limit(limit).offset(offset)
        
        
        results = query.all()
        
        response = []
        for product, price, discount_price, final_price, ts in results:
            response.append(ProductResponse(
                product_id=product.product_id,
                barcode=product.barcode,
                product_name=product.product_name,
                brand_name=product.brand_name,
                price=price,
                discount_price=discount_price,
                final_price=final_price,
                currency="ILS",
                last_updated=ts
            ))
        
        return response
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/barcode/{barcode}", response_model=List[PriceComparisonResponse])
async def get_prices_by_barcode(
    barcode: str,
    db: Session = Depends(get_db)
) -> List[PriceComparisonResponse]:
    """
    Get all prices for a product with the specified barcode across different supermarkets.
    Results are sorted by price (lowest first) for easy price comparison.
    
    Args:
        barcode: The product barcode to search for
    """
    try:
        
        product = db.query(Product).filter(Product.barcode == barcode).first()
        
        if not product:
            raise HTTPException(
                status_code=404,
                detail=f"Product with barcode '{barcode}' not found"
            )
        
       
        latest_prices_subquery = db.query(
            Price.branch_id,
            func.max(Price.ts).label("latest_ts")
        ).filter(
            Price.product_id == product.product_id
        ).group_by(Price.branch_id).subquery()
        
       
        results = db.query(
            Price,
            Branch
        ).join(
            Branch
        ).join(
            latest_prices_subquery,
            and_(
                Price.branch_id == latest_prices_subquery.c.branch_id,
                Price.ts == latest_prices_subquery.c.latest_ts
            )
        ).filter(
            Price.product_id == product.product_id
        ).order_by(
            Price.final_price.asc()  
        ).all()
        
       
        response = []
        for price, branch in results:
            
            savings = None
            if price.discount_price is not None:
                savings = price.price - price.discount_price
            
            response.append(PriceComparisonResponse(
               
                product_id=product.product_id,
                barcode=product.barcode,
                product_name=product.product_name,
                brand_name=product.brand_name,
               
                branch_id=branch.branch_id,
                provider=branch.provider,
                branch_name=branch.name,
                city=branch.city,
                
                price=price.price,
                discount_price=price.discount_price,
                final_price=price.final_price,
                currency="ILS",
                savings=savings,
               
                last_updated=price.ts
            ))
        
        if not response:
            raise HTTPException(
                status_code=404,
                detail=f"No prices found for product with barcode '{barcode}'"
            )
        
        return response
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
