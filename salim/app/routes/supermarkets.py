"""Supermarket API routes."""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, Path, status
from app.services.product_service import product_service
from app.schemas.product import (
    ProductListResponse,
    SupermarketListResponse,
    ErrorResponse
)

router = APIRouter(prefix="/api/v1/supermarkets", tags=["supermarkets"])


@router.get(
    "/{supermarket_id}/products",
    response_model=ProductListResponse,
    status_code=status.HTTP_200_OK,
    summary="Get products from a specific supermarket",
    description="Returns all products from a specific supermarket chain. Optionally search within product names.",
    responses={
        404: {"model": ErrorResponse, "description": "No products found"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    }
)
async def get_supermarket_products(
    supermarket_id: str = Path(..., description="The supermarket chain ID"),
    search: Optional[str] = Query(None, description="Search in product names", min_length=2)
):
    """Get products from a specific supermarket chain."""
    try:
        if search:
            # Search products in specific supermarket
            products = product_service.search_products_by_supermarket_and_name(
                chain_id=supermarket_id,
                search_query=search
            )
        else:
            # Get all products from specific supermarket
            products = product_service.get_products_by_supermarket(supermarket_id)
        
        if not products:
            if search:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No products found in supermarket {supermarket_id} matching search '{search}'"
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No products found for supermarket {supermarket_id}"
                )
        
        return ProductListResponse(products=products, total=len(products))
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch products for supermarket: {str(e)}"
        )


@router.get(
    "/",
    response_model=SupermarketListResponse,
    status_code=status.HTTP_200_OK,
    summary="Get all supermarkets",
    description="Returns list of all available supermarket chains.",
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"}
    }
)
async def get_supermarkets():
    """Get all available supermarkets."""
    try:
        supermarkets = product_service.get_all_supermarkets()
        
        return SupermarketListResponse(
            supermarkets=supermarkets,
            total=len(supermarkets)
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch supermarkets: {str(e)}"
        )
