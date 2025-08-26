"""Product API routes."""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, status
from app.services.product_service import product_service
from app.schemas.product import (
    ProductListResponse,
    ProductResponse,
    PromotionListResponse,
    StoreListResponse,
    ErrorResponse
)

router = APIRouter(prefix="/api/v1/products", tags=["products"])


@router.get(
    "/{item_code}",
    response_model=ProductListResponse,
    status_code=status.HTTP_200_OK,
    summary="Get product by barcode",
    description="Returns all store occurrences of a product by barcode, including price and promotion information.",
    responses={
        404: {"model": ErrorResponse, "description": "Product not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    }
)
async def get_product_by_barcode(item_code: str):
    """Get product by barcode across all stores."""
    try:
        products = product_service.get_products_by_barcode(item_code)
        
        if not products:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Product not found with the specified barcode"
            )
        
        return ProductListResponse(products=products, total=len(products))
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch product: {str(e)}"
        )


@router.get(
    "/",
    response_model=ProductListResponse,
    status_code=status.HTTP_200_OK,
    summary="Search products by name",
    description="Case-insensitive search on product name. Returns all matching store occurrences including price and promotion info.",
    responses={
        404: {"model": ErrorResponse, "description": "No products found"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    }
)
async def search_products_by_name(
    q: str = Query(..., description="Search query - substring to search within product name", min_length=2)
):
    """Search products by name (case-insensitive)."""
    try:
        products = product_service.search_products_by_name(q)
        
        if not products:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No products found matching the search query"
            )
        
        return ProductListResponse(products=products, total=len(products))
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search products: {str(e)}"
        )
