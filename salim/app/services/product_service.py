"""Product service layer for business logic."""
from typing import List, Optional, Dict, Any
from app.core.database import db_client
from app.models.product import Product, Promotion, Store
from app.schemas.product import ProductResponse, SupermarketResponse
from app.utils.helpers import parse_float, get_today_str, is_date_in_range


class ProductService:
    """Service class for product-related business logic."""
    
    def __init__(self):
        self.db = db_client.client
    
    def get_products_by_barcode(self, item_code: str) -> List[ProductResponse]:
        """
        Get all store occurrences of a product by barcode.
        
        Args:
            item_code: Product barcode
            
        Returns:
            List of product responses with promotion info
            
        Raises:
            Exception: If database query fails
        """
        try:
            # Fetch price data
            price_data = (
                self.db.table("prices")
                .select("item_code,item_name,qty_price,chain_id,company_name,store_id,store_city,store_address")
                .eq("item_code", item_code)
                .execute()
            ).data or []
            
            if not price_data:
                return []
            
            # Build response with promotion info
            results = []
            for row in price_data:
                promotion = self._get_active_promotion_for_item(
                    item_code=row["item_code"],
                    chain_id=row["chain_id"],
                    store_id=row["store_id"]
                )
                
                product_response = self._build_product_response(row, promotion)
                results.append(product_response)
            
            return results
            
        except Exception as e:
            raise Exception(f"Failed to fetch products by barcode: {str(e)}")
    
    def search_products_by_name(self, query: str) -> List[ProductResponse]:
        """
        Search products by name (case-insensitive).
        
        Args:
            query: Search query string
            
        Returns:
            List of matching product responses with promotion info
            
        Raises:
            Exception: If database query fails
        """
        try:
            # Fetch price data with name search
            price_data = (
                self.db.table("prices")
                .select("item_code,item_name,qty_price,chain_id,company_name,store_id,store_city,store_address")
                .ilike("item_name", f"%{query}%")
                .execute()
            ).data or []
            
            if not price_data:
                return []
            
            # Build response with promotion info
            results = []
            for row in price_data:
                promotion = self._get_active_promotion_for_item(
                    item_code=row["item_code"],
                    chain_id=row["chain_id"],
                    store_id=row["store_id"]
                )
                
                product_response = self._build_product_response(row, promotion)
                results.append(product_response)
            
            return results
            
        except Exception as e:
            raise Exception(f"Failed to search products by name: {str(e)}")
    
    def get_promotions_sample(self, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Get a sample of promotions for debugging.
        
        Args:
            limit: Maximum number of promotions to return
            
        Returns:
            List of promotion data
            
        Raises:
            Exception: If database query fails
        """
        try:
            data = self.db.table("promotions").select("*").limit(limit).execute().data
            return data or []
        except Exception as e:
            raise Exception(f"Failed to fetch promotions sample: {str(e)}")
    
    def get_stores(self, supermarket_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get unique stores, optionally filtered by supermarket chain.
        
        Args:
            supermarket_id: Optional supermarket chain ID to filter by
            
        Returns:
            List of unique stores
            
        Raises:
            Exception: If database query fails
        """
        try:
            query = self.db.table("prices").select("store_id,chain_id,store_address")
            if supermarket_id:
                query = query.eq("chain_id", supermarket_id)
            
            rows = query.execute().data or []
            
            # Deduplicate stores
            seen = set()
            unique_stores = []
            for row in rows:
                key = (row.get("store_id"), row.get("chain_id"), row.get("store_address"))
                if key not in seen:
                    seen.add(key)
                    unique_stores.append({
                        "store_id": key[0],
                        "chain_id": key[1],
                        "store_address": key[2]
                    })
            
            return unique_stores
            
        except Exception as e:
            raise Exception(f"Failed to fetch stores: {str(e)}")
    
    def get_products_by_supermarket(self, chain_id: str) -> List[ProductResponse]:
        """
        Get all products from a specific supermarket chain.
        
        Args:
            chain_id: Supermarket chain identifier
            
        Returns:
            List of product responses with promotion info
            
        Raises:
            Exception: If database query fails
        """
        try:
            # Fetch all products for the chain
            price_data = (
                self.db.table("prices")
                .select("item_code,item_name,qty_price,chain_id,company_name,store_id,store_city,store_address")
                .eq("chain_id", chain_id)
                .execute()
            ).data or []
            
            if not price_data:
                return []
            
            # Build response with promotion info
            results = []
            for row in price_data:
                promotion = self._get_active_promotion_for_item(
                    item_code=row["item_code"],
                    chain_id=row["chain_id"],
                    store_id=row["store_id"]
                )
                
                product_response = self._build_product_response(row, promotion)
                results.append(product_response)
            
            return results
            
        except Exception as e:
            raise Exception(f"Failed to fetch products for supermarket: {str(e)}")
    
    def search_products_by_supermarket_and_name(self, chain_id: str, search_query: str) -> List[ProductResponse]:
        """
        Search products by name within a specific supermarket chain.
        
        Args:
            chain_id: Supermarket chain identifier
            search_query: Search query string
            
        Returns:
            List of matching product responses with promotion info
            
        Raises:
            Exception: If database query fails
        """
        try:
            # Search products within specific chain
            price_data = (
                self.db.table("prices")
                .select("item_code,item_name,qty_price,chain_id,company_name,store_id,store_city,store_address")
                .eq("chain_id", chain_id)
                .ilike("item_name", f"%{search_query}%")
                .execute()
            ).data or []
            
            if not price_data:
                return []
            
            # Build response with promotion info
            results = []
            for row in price_data:
                promotion = self._get_active_promotion_for_item(
                    item_code=row["item_code"],
                    chain_id=row["chain_id"],
                    store_id=row["store_id"]
                )
                
                product_response = self._build_product_response(row, promotion)
                results.append(product_response)
            
            return results
            
        except Exception as e:
            raise Exception(f"Failed to search products in supermarket: {str(e)}")
    
    def get_all_supermarkets(self) -> List[SupermarketResponse]:
        """
        Get all available supermarket chains.
        
        Returns:
            List of unique supermarket chains with metadata
            
        Raises:
            Exception: If database query fails
        """
        try:
            # Get unique chains with company names
            chains_data = (
                self.db.table("prices")
                .select("chain_id,company_name")
                .execute()
            ).data or []
            
            # Deduplicate by chain_id
            seen_chains = set()
            unique_chains = []
            for row in chains_data:
                chain_id = row.get("chain_id")
                if chain_id and chain_id not in seen_chains:
                    seen_chains.add(chain_id)
                    unique_chains.append(SupermarketResponse(
                        supermarket_id=chain_id,
                        chain_id=chain_id,
                        company_name=row.get("company_name")
                    ))
            
            return unique_chains
            
        except Exception as e:
            raise Exception(f"Failed to fetch supermarkets: {str(e)}")
    
    def _get_active_promotion_for_item(self, item_code: str, chain_id: str, store_id: str) -> Optional[Dict[str, Any]]:
        """
        Get active promotion for a specific item in a specific store.
        
        Args:
            item_code: Product barcode
            chain_id: Chain identifier
            store_id: Store identifier
            
        Returns:
            Active promotion data or None
        """
        try:
            today = get_today_str()
            
            # Fetch promotions for the item
            promotions_data = (
                self.db.table("promotions")
                .select(
                    "promotion_id,promotion_description,discount_rate,reward_type,"
                    "promotion_start_date,promotion_end_date,additional_is_active,"
                    "item_code,chain_id,store_id"
                )
                .eq("item_code", item_code)
                .eq("chain_id", chain_id)
                .eq("store_id", store_id)
                .eq("additional_is_active", True)
                .execute()
            ).data or []
            
            # Filter for active promotions (within date range)
            active_promotions = []
            for promo in promotions_data:
                if is_date_in_range(
                    promo.get("promotion_start_date"),
                    promo.get("promotion_end_date"),
                    today
                ):
                    active_promotions.append(promo)
            
            # Return first active promotion (can be refined by priority/recency)
            return active_promotions[0] if active_promotions else None
            
        except Exception:
            return None
    
    def _build_product_response(self, price_row: Dict[str, Any], promotion_row: Optional[Dict[str, Any]]) -> ProductResponse:
        """
        Build a ProductResponse from price and promotion data.
        
        Args:
            price_row: Price data from database
            promotion_row: Promotion data or None
            
        Returns:
            ProductResponse object
        """
        return ProductResponse(
            item_code=price_row.get("item_code", ""),
            item_name=price_row.get("item_name", ""),
            store_id=price_row.get("store_id", ""),
            chain_id=price_row.get("chain_id", ""),
            has_promotion=promotion_row is not None,
            discount_rate=parse_float((promotion_row or {}).get("discount_rate"), 0.0),
            price=parse_float(price_row.get("qty_price"), 0.0),
            store_address=price_row.get("store_address")
        )


# Global service instance
product_service = ProductService()
