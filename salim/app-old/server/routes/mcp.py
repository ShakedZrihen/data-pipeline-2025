from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import logging
from ..database import get_db
from ..models import Product, Supermarket
from ..schemas import ProductResponse, PriceComparisonResponse
from sqlalchemy.orm import Session
import json

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/mcp",
    tags=["mcp"],
    responses={404: {"description": "Not found"}},
)

# Pydantic models for MCP API
class MCPToolSchema(BaseModel):
    type: str
    properties: Optional[Dict[str, Any]] = None
    required: Optional[List[str]] = None

class MCPTool(BaseModel):
    name: str
    description: Optional[str] = None
    inputSchema: Optional[MCPToolSchema] = None

class MCPServerInfo(BaseModel):
    name: str
    version: str
    description: str
    author: Optional[str] = None
    capabilities: Optional[List[str]] = None
    protocol_version: Optional[str] = None
    tools_count: Optional[int] = None

class MCPToolResult(BaseModel):
    content: List[Dict[str, Any]]
    isError: Optional[bool] = False

class MCPToolRequest(BaseModel):
    arguments: Optional[Dict[str, Any]] = {}

# MCP Server metadata
MCP_SERVER_INFO = MCPServerInfo(
    name="shopping-mcp-server",
    version="0.1.0",
    description="Shopping comparison MCP server for Israeli supermarkets",
    author="Salim Shopping Assistant",
    capabilities=["search_product", "compare_results", "find_best_basket", "get_stores", "get_store_info"],
    protocol_version="2024-11-05",
    tools_count=5
)

# Tool definitions matching the Node.js server
MCP_TOOLS = [
    MCPTool(
        name="search_product",
        description="Search for products by name in Israeli supermarkets. Returns detailed product information including IDs, barcodes, and metadata.",
        inputSchema=MCPToolSchema(
            type="object",
            properties={
                "product_name": {
                    "type": "string",
                    "description": "The product name to search for (Hebrew/English)"
                }
            },
            required=["product_name"]
        )
    ),
    MCPTool(
        name="compare_results",
        description="Compare prices for a specific product across different stores near a location. Gets price comparison data from multiple Israeli supermarket chains.",
        inputSchema=MCPToolSchema(
            type="object",
            properties={
                "product_id": {
                    "type": "string",
                    "description": "The product ID or barcode (from search results)"
                },
                "shopping_address": {
                    "type": "string",
                    "description": "Israeli city or address for location-based pricing"
                }
            },
            required=["product_id", "shopping_address"]
        )
    ),
    MCPTool(
        name="find_best_basket",
        description="Find the best shopping basket combinations across multiple stores. Analyzes multiple products and finds the most cost-effective shopping baskets by comparing total prices across different Israeli supermarket chains.",
        inputSchema=MCPToolSchema(
            type="object",
            properties={
                "products": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Array of product names to include in the basket"
                },
                "shopping_address": {
                    "type": "string",
                    "description": "Israeli city or address for location-based basket optimization"
                }
            },
            required=["products", "shopping_address"]
        )
    ),
    MCPTool(
        name="get_stores",
        description="Get list of all available supermarket stores with their details including location, branches, and contact information.",
        inputSchema=MCPToolSchema(
            type="object",
            properties={
                "city": {
                    "type": "string",
                    "description": "Filter stores by city (optional)"
                },
                "name": {
                    "type": "string", 
                    "description": "Filter stores by supermarket name (optional)"
                }
            },
            required=[]
        )
    ),
    MCPTool(
        name="get_store_info",
        description="Get detailed information about a specific supermarket store including address, branches, and available product categories.",
        inputSchema=MCPToolSchema(
            type="object",
            properties={
                "store_id": {
                    "type": "integer",
                    "description": "The supermarket store ID to get information for"
                }
            },
            required=["store_id"]
        )
    )
]

def handle_search_product(product_name: str, db: Session) -> MCPToolResult:
    """Handle search_product MCP tool call"""
    try:
        if not product_name or product_name.strip() == "":
            raise ValueError("Product name is required for search")

        logger.info(f"🔍 MCP Server: Searching for product in Salim API: {product_name}")
        
        # Search for products using database query
        query = db.query(Product)
        query = query.filter(Product.canonical_name.ilike(f"%{product_name.strip()}%"))
        results = query.limit(10).all()
        
        logger.info(f"📦 MCP Server: Found {len(results) if results else 0} products in Salim database")
        
        # Transform results to match Node.js format
        transformed_results = []
        if results:
            for product in results:
                transformed_results.append({
                    "id": product.product_id,
                    "barcode": product.barcode,
                    "name": product.canonical_name,
                    "brand": product.brand,
                    "category": product.category,
                    "price": float(product.price) if product.price else 0,
                    "promo_price": float(product.promo_price) if product.promo_price else None,
                    "promo_text": product.promo_text,
                    "supermarket_id": product.supermarket_id,
                    "size_value": product.size_value,
                    "size_unit": product.size_unit,
                    "currency": product.currency,
                    "in_stock": product.in_stock
                })
        
        return MCPToolResult(
            content=[{
                "type": "text",
                "text": json.dumps(transformed_results, ensure_ascii=False)
            }],
            isError=False
        )
        
    except Exception as error:
        logger.error(f"Error in handle_search_product: {error}")
        return MCPToolResult(
            content=[{
                "type": "text", 
                "text": f"Error: {str(error)}"
            }],
            isError=True
        )

def handle_compare_results(product_id: str, shopping_address: str, db: Session) -> MCPToolResult:
    """Handle compare_results MCP tool call"""
    try:
        if not product_id or str(product_id).strip() == "":
            raise ValueError("Product ID or barcode is required for price comparison")

        logger.info(f"💰 MCP Server: Comparing prices in Salim API for product: {product_id} near: {shopping_address}")
        
        comparison_data = None
        
        # Try barcode lookup first
        try:
            results = db.query(
                Product.product_id,
                Product.supermarket_id,
                Supermarket.name.label('supermarket_name'),
                Product.canonical_name,
                Product.brand,
                Product.category,
                Product.barcode,
                Product.price,
                Product.promo_price,
                Product.promo_text,
                Product.size_value,
                Product.size_unit,
                Product.in_stock
            ).join(
                Supermarket, Product.supermarket_id == Supermarket.supermarket_id
            ).filter(
                Product.barcode == product_id.strip()
            ).all()
            
            if results:
                comparison_data = results
            else:
                # Try as product ID
                product = db.query(Product).filter(Product.product_id == int(product_id)).first()
                if product:
                    results = db.query(
                        Product.product_id,
                        Product.supermarket_id,
                        Supermarket.name.label('supermarket_name'),
                        Product.canonical_name,
                        Product.brand,
                        Product.category,
                        Product.barcode,
                        Product.price,
                        Product.promo_price,
                        Product.promo_text,
                        Product.size_value,
                        Product.size_unit,
                        Product.in_stock
                    ).join(
                        Supermarket, Product.supermarket_id == Supermarket.supermarket_id
                    ).filter(
                        Product.barcode == product.barcode
                    ).all()
                    comparison_data = results
                    
        except ValueError:
            # product_id is not a number, continue with barcode search
            pass
        except Exception as error:
            raise ValueError(f"Price comparison failed: {str(error)}")
        
        if not comparison_data:
            raise ValueError("No price comparison data available")
        
        # Sort by effective price (promo_price or regular price)
        comparison_data = sorted(comparison_data, key=lambda x: x.promo_price or x.price)
            
        transformed_comparison = {
            "product_name": comparison_data[0].canonical_name if comparison_data else "Unknown Product",
            "brand": comparison_data[0].brand if comparison_data else "",
            "category": comparison_data[0].category if comparison_data else "",
            "barcode": comparison_data[0].barcode if comparison_data else "",
            "size_info": f"{comparison_data[0].size_value or ''} {comparison_data[0].size_unit or ''}".strip() if comparison_data else "",
            "shopping_location": shopping_address,
            "price_comparison": [{
                "supermarket": item.supermarket_name,
                "price": float(item.price) if item.price else 0,
                "promo_price": float(item.promo_price) if item.promo_price else None,
                "promo_text": item.promo_text,
                "savings": float(item.price - (item.promo_price or item.price)) if item.price and item.promo_price else 0,
                "in_stock": item.in_stock,
                "currency": "ILS"
            } for item in comparison_data],
            "best_price": min([float(item.promo_price or item.price) for item in comparison_data]) if comparison_data else 0,
            "cheapest_store": comparison_data[0].supermarket_name if comparison_data else "",
            "total_stores_checked": len(comparison_data),
            "comparison_timestamp": "2024-01-01T00:00:00Z"
        }
        
        return MCPToolResult(
            content=[{
                "type": "text",
                "text": json.dumps(transformed_comparison, ensure_ascii=False)
            }],
            isError=False
        )
        
    except Exception as error:
        logger.error(f"Error in handle_compare_results: {error}")
        return MCPToolResult(
            content=[{
                "type": "text",
                "text": f"Error: {str(error)}"
            }],
            isError=True
        )

def handle_find_best_basket(products: List[str], shopping_address: str, db: Session) -> MCPToolResult:
    """Handle find_best_basket MCP tool call"""
    try:
        if not shopping_address:
            raise ValueError("Shopping address is required for basket comparison")
        
        if not products or len(products) == 0:
            raise ValueError("Products are required for basket comparison")
            
        logger.info(f"🏪 MCP Server: Finding best basket using Salim API for products: {products} near: {shopping_address}")
        
        # Step 1: Search for each product
        product_search_results = []
        search_errors = []
        
        for product_name in products:
            try:
                search_result = handle_search_product(product_name, db)
                if not search_result.isError and search_result.content:
                    # Parse the result
                    search_data = json.loads(search_result.content[0]["text"])
                    if search_data:
                        # Find best match
                        best_match = None
                        for product in search_data:
                            if (product_name.lower() in product["name"].lower() or 
                                product["name"].lower() in product_name.lower()):
                                best_match = product
                                break
                        if not best_match and search_data:
                            best_match = search_data[0]
                        
                        if best_match:
                            product_search_results.append({
                                "productName": product_name,
                                "product": best_match
                            })
                        else:
                            search_errors.append(f"No search results for: {product_name}")
                    else:
                        search_errors.append(f"No search results for: {product_name}")
                else:
                    search_errors.append(f"Search failed for {product_name}")
            except Exception as error:
                search_errors.append(f"Search failed for {product_name}: {str(error)}")
        
        if len(product_search_results) == 0:
            raise ValueError(f"No products could be found. Errors: {', '.join(search_errors)}")
        
        # Step 2: Initialize store baskets
        supermarket_names = {
            1: "Rami Levi",
            2: "Yohananof",
            3: "Carrefour"
        }
        
        basket_data = {}
        for store_id, name in supermarket_names.items():
            basket_data[name] = {
                "supermarket_id": store_id,
                "supermarket_name": name,
                "products": [],
                "totalPrice": 0.0,
                "totalPromoPrice": 0.0,
                "totalSavings": 0.0,
                "productCount": 0,
                "location": shopping_address
            }
        
        # Step 3: Process each product for price comparison
        comparison_errors = []
        for product_result in product_search_results:
            try:
                comparison_result = handle_compare_results(
                    product_result["product"]["barcode"], 
                    shopping_address,
                    db
                )
                
                if not comparison_result.isError and comparison_result.content:
                    comparison_data = json.loads(comparison_result.content[0]["text"])
                    price_comparison = comparison_data.get("price_comparison", [])
                    
                    for price_data in price_comparison:
                        store_name = price_data["supermarket"]
                        if store_name in basket_data:
                            effective_price = price_data["promo_price"] or price_data["price"]
                            savings = price_data.get("savings", 0)
                            
                            basket_data[store_name]["products"].append({
                                "name": comparison_data["product_name"],
                                "brand": comparison_data["brand"],
                                "category": comparison_data["category"],
                                "barcode": comparison_data["barcode"],
                                "regular_price": price_data["price"],
                                "promo_price": price_data["promo_price"],
                                "effective_price": effective_price,
                                "savings": savings,
                                "promo_text": price_data["promo_text"],
                                "size_info": comparison_data["size_info"],
                                "in_stock": price_data["in_stock"]
                            })
                            
                            basket_data[store_name]["totalPrice"] += price_data["price"]
                            basket_data[store_name]["totalPromoPrice"] += effective_price
                            basket_data[store_name]["totalSavings"] += savings
                            basket_data[store_name]["productCount"] += 1
                else:
                    comparison_errors.append(f"Price comparison failed for {product_result['productName']}")
                    
            except Exception as error:
                comparison_errors.append(f"Error processing {product_result['productName']}: {str(error)}")
        
        # Step 4: Calculate final results
        complete_baskets = []
        for basket in basket_data.values():
            if basket["productCount"] == len(product_search_results):
                basket["totalPrice"] = round(basket["totalPrice"], 2)
                basket["totalPromoPrice"] = round(basket["totalPromoPrice"], 2)
                basket["totalSavings"] = round(basket["totalSavings"], 2)
                basket["averagePricePerProduct"] = round(basket["totalPromoPrice"] / basket["productCount"], 2)
                complete_baskets.append(basket)
        
        # Sort by effective price
        complete_baskets.sort(key=lambda x: x["totalPromoPrice"])
        
        result = {
            "basket_comparison": complete_baskets,
            "best_basket": complete_baskets[0] if complete_baskets else None,
            "shopping_location": shopping_address,
            "summary": {
                "total_products_requested": len(products),
                "total_products_found": len(product_search_results),
                "stores_with_complete_baskets": len(complete_baskets),
                "best_total_price": complete_baskets[0]["totalPromoPrice"] if complete_baskets else 0,
                "worst_total_price": complete_baskets[-1]["totalPromoPrice"] if complete_baskets else 0,
                "max_potential_savings": (complete_baskets[-1]["totalPromoPrice"] - complete_baskets[0]["totalPromoPrice"]) if len(complete_baskets) > 1 else 0,
                "search_errors": search_errors,
                "comparison_errors": comparison_errors
            },
            "comparison_timestamp": "2024-01-01T00:00:00Z"
        }
        
        return MCPToolResult(
            content=[{
                "type": "text",
                "text": json.dumps(result, ensure_ascii=False)
            }],
            isError=False
        )
        
    except Exception as error:
        logger.error(f"Error in handle_find_best_basket: {error}")
        return MCPToolResult(
            content=[{
                "type": "text",
                "text": f"Error: {str(error)}"
            }],
            isError=True
        )

def handle_get_stores(city: str = None, name: str = None, db: Session = None) -> MCPToolResult:
    """Handle get_stores MCP tool call"""
    try:
        logger.info(f"🏪 MCP Server: Getting stores list - city: {city}, name: {name}")
        
        # Query supermarkets with optional filters
        query = db.query(Supermarket)
        
        if city:
            query = query.filter(Supermarket.city.ilike(f"%{city}%"))
            
        if name:
            query = query.filter(Supermarket.name.ilike(f"%{name}%"))
        
        stores = query.all()
        
        logger.info(f"🏪 MCP Server: Found {len(stores)} stores")
        
        # Transform results
        transformed_stores = []
        for store in stores:
            store_data = {
                "store_id": store.supermarket_id,
                "name": store.name,
                "branch_name": store.branch_name,
                "city": store.city,
                "address": store.address,
                "website": store.website,
                "created_at": store.created_at.isoformat() if store.created_at else None
            }
            transformed_stores.append(store_data)
        
        return MCPToolResult(
            content=[{
                "type": "text",
                "text": json.dumps(transformed_stores, ensure_ascii=False)
            }],
            isError=False
        )
        
    except Exception as error:
        logger.error(f"Error in handle_get_stores: {error}")
        return MCPToolResult(
            content=[{
                "type": "text",
                "text": f"Error: {str(error)}"
            }],
            isError=True
        )

def handle_get_store_info(store_id: int, db: Session) -> MCPToolResult:
    """Handle get_store_info MCP tool call"""
    try:
        logger.info(f"🏪 MCP Server: Getting store info for store_id: {store_id}")
        
        # Get store details
        store = db.query(Supermarket).filter(Supermarket.supermarket_id == store_id).first()
        
        if not store:
            raise ValueError(f"Store with ID {store_id} not found")
        
        # Get product count and categories for this store
        product_count = db.query(Product).filter(Product.supermarket_id == store_id).count()
        categories = db.query(Product.category).filter(
            Product.supermarket_id == store_id,
            Product.category.isnot(None)
        ).distinct().all()
        
        # Get some product statistics
        promo_count = db.query(Product).filter(
            Product.supermarket_id == store_id,
            Product.promo_price.isnot(None)
        ).count()
        
        store_info = {
            "store_id": store.supermarket_id,
            "name": store.name,
            "branch_name": store.branch_name,
            "city": store.city,
            "address": store.address,
            "website": store.website,
            "created_at": store.created_at.isoformat() if store.created_at else None,
            "statistics": {
                "total_products": product_count,
                "products_on_sale": promo_count,
                "sale_percentage": round((promo_count / product_count * 100), 2) if product_count > 0 else 0,
                "categories_available": len(categories),
                "category_list": [cat[0] for cat in categories if cat[0]]
            }
        }
        
        logger.info(f"✅ MCP Server: Successfully got store info for {store.name}")
        
        return MCPToolResult(
            content=[{
                "type": "text",
                "text": json.dumps(store_info, ensure_ascii=False)
            }],
            isError=False
        )
        
    except Exception as error:
        logger.error(f"Error in handle_get_store_info: {error}")
        return MCPToolResult(
            content=[{
                "type": "text",
                "text": f"Error: {str(error)}"
            }],
            isError=True
        )

@router.get("/server/info")
async def get_server_info():
    """Get MCP server information"""
    return {"server": MCP_SERVER_INFO}

@router.get("/tools")
async def get_tools():
    """Get available MCP tools and server info"""
    return {
        "server": MCP_SERVER_INFO,
        "tools": [tool.dict() for tool in MCP_TOOLS]
    }

@router.post("/tools/{tool_name}")
def execute_tool(tool_name: str, request: MCPToolRequest, db: Session = Depends(get_db)):
    """Execute a specific MCP tool"""
    try:
        args = request.arguments or {}
        
        if tool_name == "search_product":
            return handle_search_product(args.get("product_name", ""), db)
        elif tool_name == "compare_results":
            return handle_compare_results(
                args.get("product_id", ""),
                args.get("shopping_address", ""),
                db
            )
        elif tool_name == "find_best_basket":
            return handle_find_best_basket(
                args.get("products", []),
                args.get("shopping_address", ""),
                db
            )
        elif tool_name == "get_stores":
            return handle_get_stores(
                city=args.get("city"),
                name=args.get("name"),
                db=db
            )
        elif tool_name == "get_store_info":
            return handle_get_store_info(
                store_id=args.get("store_id"),
                db=db
            )
        else:
            raise HTTPException(status_code=404, detail=f"Unknown tool: {tool_name}")
            
    except Exception as error:
        logger.error(f"Error executing tool {tool_name}: {error}")
        return MCPToolResult(
            content=[{
                "type": "text",
                "text": f"Error: {str(error)}"
            }],
            isError=True
        )

@router.get("/health")
def mcp_health_check():
    """Health check endpoint for MCP"""
    return {
        "status": "healthy",
        "mcpConnected": True,
        "server": MCP_SERVER_INFO.dict()
    }