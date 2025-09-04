from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routes import supermarkets, products

app = FastAPI(
    title="Salim API",
    description="""
    ## 🛒 Israeli Supermarket Price Comparison API
    
    Compare prices across major Israeli supermarkets including:
    - **SuperSapir** 
    - **Yohananof** 
    - **ZolVeBegadol** 
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {
            "name": "supermarkets",
            "description": "Operations with supermarkets. Get store information and their product catalogs."
        },
        {
            "name": "products", 
            "description": "Product search, lookup, and price comparison operations. Find products by various criteria and compare prices across supermarkets."
        },
    ]
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(supermarkets.router)
app.include_router(products.router)