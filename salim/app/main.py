from fastapi import FastAPI
from routes.supermarkets import router as supermarkets_router
from routes.stores import router as stores_router
from routes.products import router as products_router
import middlewares.cors as cors_middleware
from db.db import get_conn
import uvicorn

app = FastAPI(
    title="Our swagger to query",
    docs_url="/docs",
    openapi_tags=[
        {"name": "supermarkets", "description": "Supermarkets"},
        {"name": "stores", "description": "stores"},
        {"name": "products", "description": "items"},
        {"name": "meta", "description": "Meta"},
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
app.include_router(products_router, prefix="/api")

if __name__ == "__main__":
    print("Docs at: http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
