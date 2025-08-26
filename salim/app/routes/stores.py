"""Store API routes."""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, status
from app.services.product_service import product_service
from app.schemas.product import StoreListResponse, ErrorResponse

router = APIRouter(prefix="/api/v1/stores", tags=["stores"])


@router.get(
    "/",
    response_model=StoreListResponse,
    status_code=status.HTTP_200_OK,
    summary="Get stores",
    description="Returns unique stores from the database. Optionally filter by chain ID.",
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"}
    }
)
async def get_stores(
    chain_id: Optional[str] = Query(None, description="Optional chain ID to filter stores by")
):
    """Get stores, optionally filtered by chain."""
    try:
        stores_data = product_service.get_stores(chain_id)
        
        return StoreListResponse(stores=stores_data, total=len(stores_data))
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch stores: {str(e)}"
        )
