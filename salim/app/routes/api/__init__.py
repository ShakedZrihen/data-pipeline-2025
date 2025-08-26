# API routes package 
from fastapi import APIRouter
from .health import router as health_router

# Create main API router
api_router = APIRouter(prefix="/api/v1")

# Include all route modules
api_router.include_router(health_router) 