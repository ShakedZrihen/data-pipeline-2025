from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes.api import router as api_router
import uvicorn

# Create the application
app = FastAPI(
    title="Salim API - Food Management System",
    description="Application for managing food data, prices and promotions",
    version="1.0.0",
    docs_url="/docs",    # Automatic documentation page
    redoc_url="/redoc"   # Alternative documentation page
)

# Add CORS middleware - allows any website to access the server
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],         # All domains (not recommended for production)
    allow_credentials=True,
    allow_methods=["*"],         # All operations (GET, POST, PUT, DELETE)
    allow_headers=["*"],
)

# Basic route - home page
@app.get("/")
async def root():
    """Home route - returns welcome message"""
    return {
        "message": "Welcome to Salim Application!",
        "description": "System for managing food data and promotions",
        "endpoints": {
            "docs": "/docs",
            "health": "/health", 
            "api": "/api/*"
        }
}

# Health route - check if server is working
@app.get("/health")
async def health_check():
    """Server health check"""
    return {"status": "healthy", "message": "Server is running properly"}

# Add API routes
app.include_router(api_router, prefix="/api")

# Run the server
if __name__ == "__main__":
    print("🚀 Starting the server...")
    print("📋 Documentation available at: http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
