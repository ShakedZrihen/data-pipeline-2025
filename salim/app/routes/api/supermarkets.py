"""
Supermarkets API endpoints.
Provides endpoints for listing supermarket chains and their branches.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, select
from typing import List, Optional

from ...database import get_db
from ...models import Branch, Product, Price
from ...schemas import SupermarketResponse, BranchBase, ProductWithBranchResponse

router = APIRouter(prefix="/supermarkets", tags=["supermarkets"])


@router.get("/", response_model=List[SupermarketResponse])
async def get_supermarkets(
    db: Session = Depends(get_db)
) -> List[SupermarketResponse]:
    """
    Get all supermarkets (unique providers) with branch counts.
    Returns a list of supermarket chains operating in the system.
    """
    try:
        # Query to get unique providers with branch count
        supermarkets = db.query(
            Branch.provider,
            func.count(Branch.branch_id).label("branch_count")
        ).group_by(Branch.provider).order_by(Branch.provider).all()
        
        # Transform to response model
        result = []
        for provider, branch_count in supermarkets:
            result.append(SupermarketResponse(
                provider=provider,
                branch_count=branch_count
            ))
        
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/{provider}", response_model=SupermarketResponse)
async def get_supermarket_details(
    provider: str,
    db: Session = Depends(get_db)
) -> SupermarketResponse:
    """
    Get details for a specific supermarket chain including all branches.
    
    Args:
        provider: The supermarket chain name (e.g., 'shufersal', 'victory')
    """
    try:
        # Get all branches for this provider
        branches = db.query(Branch).filter(
            Branch.provider == provider
        ).all()
        
        if not branches:
            raise HTTPException(
                status_code=404, 
                detail=f"Supermarket '{provider}' not found"
            )
        
        # Convert branches to BranchBase models
        branch_models = [BranchBase.model_validate(branch) for branch in branches]
        
        return SupermarketResponse(
            provider=provider,
            branch_count=len(branches),
            branches=branch_models
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/{provider}/products", response_model=List[ProductWithBranchResponse])
async def get_supermarket_products(
    provider: str,
    search: Optional[str] = Query(None, description="Search in product names"),
    limit: int = Query(100, ge=1, le=500, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: Session = Depends(get_db)
) -> List[ProductWithBranchResponse]:
    """
    Get products from a specific supermarket with their prices.
    
    Args:
        provider: The supermarket chain name
        search: Optional search term for product names (supports Hebrew)
        limit: Maximum number of results to return
        offset: Offset for pagination
    """
    try:
        # Check if provider exists
        provider_exists = db.query(Branch).filter(
            Branch.provider == provider
        ).first()
        
        if not provider_exists:
            raise HTTPException(
                status_code=404,
                detail=f"Supermarket '{provider}' not found"
            )
        
        # Build the query - get latest prices for products in this supermarket
        # Using a subquery to get the latest price per product per branch
        subquery = db.query(
            Price.product_id,
            Price.branch_id,
            func.max(Price.ts).label("latest_ts")
        ).join(
            Branch
        ).filter(
            Branch.provider == provider
        ).group_by(
            Price.product_id,
            Price.branch_id
        ).subquery()
        
        # Main query joining with the latest prices
        query = db.query(
            Product,
            Price,
            Branch
        ).join(
            Price, Product.product_id == Price.product_id
        ).join(
            Branch, Price.branch_id == Branch.branch_id
        ).join(
            subquery,
            (Price.product_id == subquery.c.product_id) &
            (Price.branch_id == subquery.c.branch_id) &
            (Price.ts == subquery.c.latest_ts)
        ).filter(
            Branch.provider == provider
        )
        
        # Apply search filter if provided
        if search:
            query = query.filter(
                Product.product_name.ilike(f"%{search}%")
            )
        
        # Apply pagination
        query = query.limit(limit).offset(offset)
        
        # Execute query
        results = query.all()
        
        # Transform to response model
        response = []
        for product, price, branch in results:
            response.append(ProductWithBranchResponse(
                # Product info
                product_id=product.product_id,
                barcode=product.barcode,
                product_name=product.product_name,
                brand_name=product.brand_name,
                # Price info
                price=price.price,
                discount_price=price.discount_price,
                final_price=price.final_price,
                currency="ILS",
                # Branch info
                branch_id=branch.branch_id,
                provider=branch.provider,
                branch_name=branch.name,
                city=branch.city,
                # Timestamp
                last_updated=price.ts
            ))
        
        return response
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
