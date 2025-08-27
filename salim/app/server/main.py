from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

from .routes import supermarkets, products, utils

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Salim API",
    description="""
    ## 🛒 Israeli Supermarket Price Comparison API
    
    Compare prices across major Israeli supermarkets including:
    - **Rami Levi** - Generally offering competitive prices
    - **Yohananof** - Premium supermarket chain
    - **Carrefour** - International retail chain
    
    ### 🔍 Key Features:
    - **Product Search** - Find products by name, category, or brand
    - **Price Comparison** - Compare same products across different stores
    - **Barcode Lookup** - Scan barcodes to find products instantly
    - **Live Data** - Real-time pricing information
    
    ### 📊 Sample Data:
    - **3,000+ products** across all categories
    - **12.2% products on sale** with promotional pricing
    - **Foundation products** like milk, bread, eggs available in all stores
    
    ### 🏷️ Price Examples:
    - Milk 1L: ₪5.34 (Rami Levi) vs ₪6.03 (Carrefour)
    - White Bread: ₪4.21 (Rami Levi) vs ₪4.81 (Yohananof)
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
        {
            "name": "utilities",
            "description": "Utility endpoints for categories, brands, statistics and health checks."
        }
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

@app.get("/")
def read_root():
    return {"message": "Welcome to Salim API - Israeli Supermarket Price Comparison"}

# Include routers
app.include_router(supermarkets.router)
app.include_router(products.router)
app.include_router(utils.router)