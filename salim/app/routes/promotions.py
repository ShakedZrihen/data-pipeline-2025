"""Promotion API routes."""
from fastapi import APIRouter, HTTPException, Query, status
from app.services.product_service import product_service
from app.schemas.product import PromotionListResponse, ErrorResponse

router = APIRouter(prefix="/api/v1/promotions", tags=["promotions"])


@router.get(
    "/",
    response_model=PromotionListResponse,
    status_code=status.HTTP_200_OK,
    summary="Get promotions sample",
    description="Returns a sample of promotions for debugging purposes.",
    responses={
        500: {"model": ErrorResponse, "description": "Internal server error"}
    }
)
async def get_promotions(
    limit: int = Query(25, description="Maximum number of promotions to return", ge=1, le=100)
):
    """Get a sample of promotions."""
    try:
        promotions_data = product_service.get_promotions_sample(limit)
        
        return PromotionListResponse(promotions=promotions_data, total=len(promotions_data))
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch promotions: {str(e)}"
        )
