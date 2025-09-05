import os
import requests
from typing import List, Dict, Optional
from fastapi import HTTPException


class SupabaseService:
    """Service for interacting with Supabase database"""
    
    def __init__(self):
        self.supabase_url = os.environ.get('SUPABASE_URL')
        self.supabase_key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.supabase_url or not self.supabase_key:
            raise Exception("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables")
        
        self.headers = {
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}',
            'Content-Type': 'application/json'
        }
    
    def execute_query(self, endpoint: str, params: Optional[Dict] = None) -> List[Dict]:
        """Execute a query against Supabase REST API"""
        url = f"{self.supabase_url}/rest/v1/{endpoint}"
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Database query failed: {e}")
            raise HTTPException(status_code=500, detail="Database query failed")
    
    def get_supermarkets(self) -> List[Dict]:
        """Get all unique supermarkets (provider + branch combinations)"""
        params = {
            'select': 'provider,branch,created_at',
            'order': 'provider.asc,branch.asc'
        }
        
        data = self.execute_query('supermarket_data', params)
        
        # Deduplicate based on provider + branch
        supermarkets = {}
        for item in data:
            key = (item['provider'], item['branch'])
            if key not in supermarkets:
                supermarkets[key] = item
        
        return list(supermarkets.values())
    
    def get_supermarket_products(self, provider: str, branch: str, search: Optional[str] = None) -> List[Dict]:
        """Get products from a specific supermarket"""
        params = {
            'provider': f'eq.{provider}',
            'branch': f'eq.{branch}',
            'select': 'id,provider,branch,product_name,manufacturer,price,unit,category,is_promotion,is_kosher,file_timestamp,created_at',
            'order': 'product_name.asc',
            'limit': '1000'
        }
        
        if search:
            params['product_name'] = f'ilike.*{search}*'
        
        return self.execute_query('supermarket_data', params)
    
    def search_products(
        self,
        name: Optional[str] = None,
        promo: Optional[bool] = None,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
        provider: Optional[str] = None,
        branch: Optional[str] = None
    ) -> List[Dict]:
        """Search products with various filters"""
        params = {
            'select': 'id,provider,branch,product_name,manufacturer,price,unit,category,is_promotion,is_kosher,file_timestamp,created_at',
            'order': 'product_name.asc',
            'limit': '1000'
        }
        
        # Apply filters
        if name:
            params['product_name'] = f'ilike.*{name}*'
        
        if promo is not None:
            params['is_promotion'] = f'eq.{promo}'
        
        if min_price is not None:
            params['price'] = f'gte.{min_price}'
        
        if max_price is not None:
            if 'price' in params:
                # This is a limitation of Supabase REST API - we can't combine gte and lte easily
                # We'll apply min_price filter in the query and max_price in post-processing
                pass
            else:
                params['price'] = f'lte.{max_price}'
        
        if provider:
            params['provider'] = f'eq.{provider}'
        
        if branch:
            params['branch'] = f'eq.{branch}'
        
        data = self.execute_query('supermarket_data', params)
        
        # Post-process for max_price if both min_price and max_price are specified
        if min_price is not None and max_price is not None:
            data = [item for item in data if min_price <= float(item['price']) <= max_price]
        
        return data


# Global database service instance
db_service = SupabaseService()