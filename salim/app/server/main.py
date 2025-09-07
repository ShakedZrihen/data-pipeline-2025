from fastapi import FastAPI
from .routes import supermarkets, products

tags_metadata = [
    {"name": "supermarkets", "description": "Supermarkets directory and their products."},
    {"name": "products", "description": "Search, filter and compare product prices."},
]

app = FastAPI(
    title="Prices API",
    description="Exposes endpoints for querying product prices with filters and comparisons.",
    version="1.0.0",
    openapi_tags=tags_metadata,
)

app.include_router(supermarkets.router)
app.include_router(products.router)