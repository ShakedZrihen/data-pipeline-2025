"""Main API router that includes all route modules."""
from fastapi import APIRouter
from app.routes.products import router as products_router
from app.routes.promotions import router as promotions_router
from app.routes.stores import router as stores_router

# Create main API router
router = APIRouter()

# Include all sub-routers
router.include_router(products_router)
router.include_router(promotions_router)
router.include_router(stores_router)
