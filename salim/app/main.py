from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes.api import api_router
import uvicorn

app = FastAPI(
    title="Salim API",
    description="A FastAPI server with PostgreSQL integration",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Welcome to Salim API!"}

# Include API routes
app.include_router(api_router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 