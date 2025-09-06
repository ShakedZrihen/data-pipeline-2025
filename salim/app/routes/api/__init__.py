# API routes package 
from fastapi import APIRouter
from .health import router as health_router
from .supermarkets import router as supermarkets_router
from .products import router as products_router


api_router = APIRouter(prefix="/api/v1")


api_router.include_router(health_router)
api_router.include_router(supermarkets_router)
api_router.include_router(products_router)
