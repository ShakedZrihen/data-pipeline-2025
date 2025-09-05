# API routes package 
from fastapi import APIRouter
from routes.api.health import router as health_router
from routes.api.supermarkets import router as supermarkets_router
from routes.api.products import router as products_router

# Create main API router
api_router = APIRouter(prefix="/api/v1")

# Include all route modules
api_router.include_router(health_router)
api_router.include_router(supermarkets_router)
api_router.include_router(products_router) 