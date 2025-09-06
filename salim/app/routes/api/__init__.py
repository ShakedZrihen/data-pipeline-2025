from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import os
from dotenv import load_dotenv
from supabase import create_client, Client
from .health import router as health_router

# Create main API router
api_router = APIRouter(prefix="/api/v1")

# Include all route modules
api_router.include_router(health_router) 

# Load environment variables
load_dotenv()

# Supabase configuration
SUPABASE_URL = os.getenv("DATABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")  # or SUPABASE_SERVICE_KEY for full access

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Pydantic models for responses
class SupermarketResponse(BaseModel):
    supermarket_id: int
    name: str
    branch_name: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    created_at: datetime

class ProductResponse(BaseModel):
    product_id: int
    supermarket_id: int
    barcode: str
    canonical_name: str
    brand: Optional[str]
    category: Optional[str]
    size_value: Optional[float]
    size_unit: Optional[str]
    price: float
    currency: str = "ILS"
    promo_price: Optional[float]
    promo_text: Optional[str]
    in_stock: bool
    collected_at: datetime

class PriceComparisonResponse(BaseModel):
    product_id: int
    supermarket_id: int
    supermarket_name: str
    canonical_name: str
    brand: Optional[str]
    category: Optional[str]
    barcode: str
    price: float
    promo_price: Optional[float]
    promo_text: Optional[str]
    size_value: Optional[float]
    size_unit: Optional[str]
    in_stock: bool
    savings: float

# Helper function to get supermarket info (mapping chain_id to supermarket data)
def get_supermarket_info(chain_id: str) -> dict:
    """Map chain_id to supermarket information"""
    supermarket_mapping = {
        # Major Israeli supermarket chains
        "7290027600007": {"name": "רמי לוי", "website": "https://www.rami-levy.co.il/"},
        "7290058140886": {"name": "שופרסל", "website": "https://www.shufersal.co.il/"},
        "7290103152017": {"name": "ויקטורי", "website": "https://www.victory.co.il/"},
        "7290876100000": {"name": "מגה", "website": "https://www.mega.co.il/"},
        "7290455000000": {"name": "יוחננוף", "website": "https://www.yochananof.co.il/"},
        "7290492000000": {"name": "קופיקס", "website": "https://www.cofix.co.il/"},
        "7290873255550": {"name": "חצי חינם", "website": "https://www.hazi-hinam.co.il/"},
        "7290100700000": {"name": "אושר עד", "website": "https://www.osher-ad.co.il/"},
        "7290661400000": {"name": "זל ביג", "website": "https://zolbig.co.il/"},
        "7290725900000": {"name": "ישר כשר", "website": "https://yshar-kasher.co.il/"},
    }
    return supermarket_mapping.get(chain_id, {"name": f"Store {chain_id}", "website": None})

# Helper function to convert database row to ProductResponse
def row_to_product(row: dict, supermarket_id: int) -> ProductResponse:
    return ProductResponse(
        product_id=row.get("id", 0),
        supermarket_id=supermarket_id,
        barcode=row.get("item_code", ""),
        canonical_name=row.get("item_name", ""),
        brand=row.get("manufacturer_name"),
        category=row.get("manufacture_country"),  # You might want to map this to actual categories
        size_value=row.get("quantity"),
        size_unit=row.get("unit_of_measure"),
        price=row.get("item_price", 0.0),
        currency="ILS",
        promo_price=None,  # Add promo logic based on your business rules
        promo_text=None,
        in_stock=row.get("item_status") != "discontinued" if row.get("item_status") else True,
        collected_at=datetime.fromisoformat(row.get("extraction_timestamp").replace("Z", "+00:00")) if row.get("extraction_timestamp") else datetime.now()
    )

# Helper function to get unique chain IDs
async def get_unique_chains():
    """Get unique chain IDs from the database"""
    try:
        response = supabase.table("prices").select("chain_id").not_.is_("chain_id", "null").execute()
        if response.data:
            # Get unique chain_ids
            unique_chains = list(set(row["chain_id"] for row in response.data if row["chain_id"]))
            return unique_chains
        return []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# Helper function to get chain_id by supermarket_id
async def get_chain_by_supermarket_id(supermarket_id: int):
    """Get chain_id by supermarket_id"""
    unique_chains = await get_unique_chains()
    if supermarket_id <= 0 or supermarket_id > len(unique_chains):
        raise HTTPException(status_code=404, detail="Supermarket not found")
    return unique_chains[supermarket_id - 1]

# Health check endpoint
@api_router.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now()}

# Supermarkets Routes
@api_router.get("/supermarkets", response_model=List[SupermarketResponse], tags=["supermarkets"])
async def get_supermarkets():
    """Get all supermarkets"""
    try:
        unique_chains = await get_unique_chains()
        
        supermarkets = []
        for i, chain_id in enumerate(unique_chains, 1):
            if chain_id:
                info = get_supermarket_info(chain_id)
                supermarkets.append(SupermarketResponse(
                    supermarket_id=i,
                    name=info["name"],
                    website=info["website"],
                    created_at=datetime.now()
                ))
        
        return supermarkets
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching supermarkets: {str(e)}")

@api_router.get("/supermarkets/{supermarket_id}", response_model=SupermarketResponse, tags=["supermarkets"])
async def get_supermarket(supermarket_id: int):
    """Get a specific supermarket by ID"""
    try:
        chain_id = await get_chain_by_supermarket_id(supermarket_id)
        info = get_supermarket_info(chain_id)
        
        return SupermarketResponse(
            supermarket_id=supermarket_id,
            name=info["name"],
            website=info["website"],
            created_at=datetime.now()
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching supermarket: {str(e)}")

@api_router.get("/supermarkets/{supermarket_id}/products", response_model=List[ProductResponse], tags=["supermarkets"])
async def get_supermarket_products(
    supermarket_id: int,
    search: Optional[str] = Query(None, description="Search in product names"),
    limit: int = Query(100, description="Maximum number of results", le=1000),
    offset: int = Query(0, description="Number of results to skip", ge=0),
):
    """Get products from a specific supermarket"""
    try:
        chain_id = await get_chain_by_supermarket_id(supermarket_id)
        
        # Build query
        query = supabase.table("prices").select("*").eq("chain_id", chain_id)
        
        # Add search filter if provided
        if search:
            query = query.ilike("item_name", f"%{search}%")
        
        # Add pagination
        query = query.range(offset, offset + limit - 1)
        
        response = query.execute()
        
        if not response.data:
            return []
        
        return [row_to_product(row, supermarket_id) for row in response.data]
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching products: {str(e)}")

# Products Routes
@api_router.get("/products", response_model=List[ProductResponse], tags=["products"])
async def search_products(
    name: Optional[str] = Query(None, description="Filter by product name"),
    promo: Optional[bool] = Query(None, description="Filter by promotion status"),
    min_price: Optional[float] = Query(None, description="Minimum price filter"),
    max_price: Optional[float] = Query(None, description="Maximum price filter"),
    supermarket_id: Optional[int] = Query(None, description="Filter by specific supermarket ID"),
    limit: int = Query(100, description="Maximum number of results", le=1000),
    offset: int = Query(0, description="Number of results to skip", ge=0),
):
    """Search products with various filters"""
    try:
        # Start with base query
        query = supabase.table("prices").select("*").not_.is_("item_name", "null")
        
        # Apply filters
        if name:
            query = query.ilike("item_name", f"%{name}%")
        
        if min_price is not None:
            query = query.gte("item_price", min_price)
        
        if max_price is not None:
            query = query.lte("item_price", max_price)
        
        if supermarket_id:
            chain_id = await get_chain_by_supermarket_id(supermarket_id)
            query = query.eq("chain_id", chain_id)
        
        # Note: promo filtering would require additional logic based on your promo detection
        if promo is not None:
            # Add your promo logic here, e.g., check for discount flags or promo text
            pass
        
        # Add pagination
        query = query.range(offset, offset + limit - 1)
        
        response = query.execute()
        
        if not response.data:
            return []
        
        # Create a mapping from chain_id to supermarket_id for efficiency
        unique_chains = await get_unique_chains()
        chain_to_id = {cid: i for i, cid in enumerate(unique_chains, 1)}
        
        products = []
        for row in response.data:
            sm_id = chain_to_id.get(row.get("chain_id"), 1)
            products.append(row_to_product(row, sm_id))
        
        return products
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching products: {str(e)}")

@api_router.get("/products/barcode/{barcode}", response_model=List[PriceComparisonResponse], tags=["products"])
async def get_products_by_barcode(barcode: str):
    """Get all products with the same barcode across different supermarkets, sorted by price"""
    try:
        # Query products with the same barcode, filter out null prices, and order by price
        response = supabase.table("prices").select("*").eq("item_code", barcode).not_.is_("item_price", "null").order("item_price").execute()
        
        if not response.data:
            raise HTTPException(status_code=404, detail="Product not found")
        
        # Get unique chains and create mapping
        unique_chains = await get_unique_chains()
        chain_to_id = {cid: i for i, cid in enumerate(unique_chains, 1)}
        
        comparisons = []
        prices = [row["item_price"] for row in response.data if row["item_price"] is not None]
        min_price = min(prices) if prices else 0
        
        for row in response.data:
            supermarket_id = chain_to_id.get(row.get("chain_id"), 1)
            supermarket_info = get_supermarket_info(row.get("chain_id", ""))
            
            item_price = row.get("item_price", 0.0)
            savings = (item_price - min_price) if item_price and min_price else 0
            
            comparisons.append(PriceComparisonResponse(
                product_id=row.get("id", 0),
                supermarket_id=supermarket_id,
                supermarket_name=supermarket_info["name"],
                canonical_name=row.get("item_name", ""),
                brand=row.get("manufacturer_name"),
                category=row.get("manufacture_country"),
                barcode=barcode,
                price=item_price,
                promo_price=None,  # Add promo logic if available
                promo_text=None,
                size_value=row.get("quantity"),
                size_unit=row.get("unit_of_measure"),
                in_stock=row.get("item_status") != "discontinued" if row.get("item_status") else True,
                savings=savings
            ))
        
        return comparisons
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching product comparison: {str(e)}")