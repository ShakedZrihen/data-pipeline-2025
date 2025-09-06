from fastapi import APIRouter

router = APIRouter(prefix="/health", tags=["health"])

@router.get("/")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "salim-api",
        "version": "1.0.0"
    }

@router.get("/detailed")
async def detailed_health_check():
    """Detailed health check endpoint"""
    return {
        "status": "healthy",
        "service": "salim-api",
        "version": "1.0.0",
        "components": {
            "api": "operational",
            "database": "operational"
        }
    } 