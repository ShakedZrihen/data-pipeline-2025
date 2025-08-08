"""
Data Normalizer for converting parsed XML data to standardized JSON format
"""

from typing import List, Dict, Any
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)


class DataNormalizer:
    """
    Normalizes parsed data from different providers into a consistent format
    """
    
    def normalize(self, parsed_data: Dict[str, Any], provider: str, branch: str, 
                  file_type: str) -> List[Dict[str, Any]]:
        """
        Normalize parsed data into the required JSON format.
        
        Args:
            parsed_data: Dictionary with 'items' and 'metadata' from XMLProcessor
            provider: Provider name (e.g., 'yohananof', 'carrefour')
            branch: Branch identifier (e.g., 'תל אביב - יפו')
            file_type: Either 'pricesFull' or 'promoFull'
            
        Returns:
            List of normalized items in the required format
        """
        items = parsed_data.get('items', [])
        metadata = parsed_data.get('metadata', {})
        
        if file_type == 'promoFull':
            return self._normalize_promotions(items, provider, branch, metadata)
        else:
            return self._normalize_prices(items, provider, branch, metadata)
    
    def _normalize_prices(self, items: List[Dict[str, Any]], provider: str, 
                          branch: str, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Normalize price items to standard format.
        
        Expected output format:
        {
            "product": "חלב תנובה 3%",
            "price": 5.9,
            "unit": "liter",
            "barcode": "7290000009358",
            "manufacturer": "תנובה",
            "store_id": "246",
            "chain_id": "7290661400001"
        }
        """
        normalized = []
        
        for item in items:
            normalized_item = {
                "product": item.get('name') or item.get('product'),
                "price": self._parse_price(item.get('price')),
                "unit": self._normalize_unit(item.get('unit') or item.get('unit_measure')),
                "barcode": item.get('barcode') or item.get('itemcode'),
                "manufacturer": item.get('manufacturer') or item.get('brand'),
                "quantity": item.get('quantity'),
                "provider": provider,
                "branch": branch,
                "store_id": metadata.get('storeid'),
                "chain_id": metadata.get('chainid'),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            # Remove None values
            normalized_item = {k: v for k, v in normalized_item.items() if v is not None}
            
            # Only add if we have at least product or barcode
            if normalized_item.get('product') or normalized_item.get('barcode'):
                normalized.append(normalized_item)
        
        return normalized
    
    def _normalize_promotions(self, items: List[Dict[str, Any]], provider: str,
                             branch: str, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Normalize promotion items to standard format.
        
        Expected output format:
        {
            "promotion_id": "11332725",
            "description": "כתף בקר כשר/ ח. טרי לה מרשה",
            "barcode": "7290000009358",
            "discount_price": 70.00,
            "min_qty": 2.00,
            "start_date": "2025-06-30",
            "end_date": "2025-08-04"
        }
        """
        normalized = []
        
        for item in items:
            normalized_item = {
                "promotion_id": item.get('promotion_id'),
                "description": item.get('description'),
                "barcode": item.get('barcode') or item.get('itemcode'),
                "discount_price": self._parse_price(item.get('discount_price')),
                "regular_price": self._parse_price(item.get('regular_price')),
                "min_qty": self._parse_quantity(item.get('min_qty')),
                "start_date": item.get('start_date'),
                "end_date": item.get('end_date'),
                "provider": provider,
                "branch": branch,
                "store_id": metadata.get('storeid'),
                "chain_id": metadata.get('chainid'),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            # Remove None values
            normalized_item = {k: v for k, v in normalized_item.items() if v is not None}
            
            # Only add if we have meaningful data
            if normalized_item.get('barcode') or normalized_item.get('promotion_id'):
                normalized.append(normalized_item)
        
        return normalized
    
    def _parse_price(self, price_value: Any) -> float:
        """Parse price value to float"""
        if price_value is None:
            return None
        
        if isinstance(price_value, (int, float)):
            return float(price_value)
        
        if isinstance(price_value, str):
            try:
                # Remove common currency symbols and spaces
                cleaned = price_value.replace('₪', '').replace(',', '').strip()
                return float(cleaned)
            except ValueError:
                logger.warning(f"Could not parse price: {price_value}")
                return None
        
        return None
    
    def _parse_quantity(self, qty_value: Any) -> float:
        """Parse quantity value to float"""
        if qty_value is None:
            return None
        
        if isinstance(qty_value, (int, float)):
            return float(qty_value)
        
        if isinstance(qty_value, str):
            try:
                return float(qty_value.replace(',', '').strip())
            except ValueError:
                return None
        
        return None
    
    def _normalize_unit(self, unit_value: str) -> str:
        """
        Normalize unit strings to standard values.
        
        Common Hebrew units:
        - י״ח = unit/piece
        - ליטר = liter
        - ק״ג = kg
        - גרם = gram
        - מ״ל = ml
        """
        if not unit_value:
            return None
        
        unit_lower = unit_value.lower().strip()
        
        # Map Hebrew units to English
        unit_mapping = {
            'י״ח': 'unit',
            'יח': 'unit',
            'יחידה': 'unit',
            'ליטר': 'liter',
            'ל': 'liter',
            'מ״ל': 'ml',
            'מל': 'ml',
            'ק״ג': 'kg',
            'קג': 'kg',
            'קילוגרם': 'kg',
            'גרם': 'gram',
            'ג': 'gram',
            'מטר': 'meter',
            'מ': 'meter'
        }
        
        # Check for exact matches
        for hebrew, english in unit_mapping.items():
            if hebrew in unit_lower:
                return english
        
        # Return original if no mapping found
        return unit_value
