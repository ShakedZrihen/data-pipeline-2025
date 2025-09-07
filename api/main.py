from fastapi import FastAPI
from .routers import supermarkets, products
from routes_enriched import router as enriched_router


app = FastAPI(
    title="Prices API",
    version="1.0.0",
    description="API לשליפת מחירי מוצרים מסופרמרקטים",
)

app.include_router(supermarkets.router)
app.include_router(products.router)
app.include_router(enriched_router)

@app.get("/")
def root():
    return {"ok": True, "docs": "/docs"}
