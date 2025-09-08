import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes.api.health import router as health_router
from .routes.api.supermarkets import router as supermarkets_router
from .routes.api.products import router as products_router
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

app.include_router(health_router)
app.include_router(supermarkets_router)
app.include_router(products_router)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)