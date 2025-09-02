from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes.api import router as api_router
import uvicorn

app = FastAPI(
    title="Salim API - Food Management System",
    description="Application for managing food data, prices and promotions",
    version="1.0.0",
    docs_url="/docs",   
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],         
    allow_credentials=True,
    allow_methods=["*"],         
    allow_headers=["*"],
)

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

@app.get("/health")
async def health_check():
    return {"status": "healthy", "message": "Server is running properly"}

app.include_router(api_router, prefix="/api")

if __name__ == "__main__":
    print("Starting the server...")
    print("Documentation available at: http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)