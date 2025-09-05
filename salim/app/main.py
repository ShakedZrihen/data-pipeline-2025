from fastapi import FastAPI
from routes.supermarkets import router as supermarkets_router
from routes.stores import router as stores_router
# from routes.products import router as products_router
import middlewares.cors as cors_middleware
from db.db import get_conn
import uvicorn

app = FastAPI(
    title="Salim API - Food Management System",
    description="Application for managing food data, prices and promotions",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {"name": "supermarkets", "description": "Supermarket (chain) directory & chain-level queries"},
        {"name": "stores", "description": "Physical store locations"},
        {"name": "products", "description": "Product search and price comparison"},
        {"name": "meta", "description": "Service metadata and health"},
    ],
)

cors_middleware.add_cors_middleware(app)

@app.get("/", tags=["meta"])
def root():
    return {"message": "Welcome", "docs": "/docs", "health": "/health"}

@app.get("/health", tags=["meta"])
def health():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()
    return {"status": "healthy"}

app.include_router(supermarkets_router, prefix="/api")
app.include_router(stores_router, prefix="/api")
# app.include_router(products_router, prefix="/api")

if __name__ == "__main__":
    print("Docs at: http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
