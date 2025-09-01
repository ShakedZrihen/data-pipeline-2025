from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routes import supermarket

app = FastAPI(
    title="Salim API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {
            "name": "supermarkets",
            "description": "Operations with supermarkets. Get store information and their product catalogs.",
        },
        {
            "name": "products",
            "description": "Product search, lookup, and price comparison operations. Find products by various criteria and compare prices across supermarkets.",
        },
    ],
)

# Add CORS middleware, in a production settings we wouldn't allow all origins, methods and headers.
# but its okay here.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(supermarket.router)
