import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class DataValidator:
    """Handles validation of normalized data"""
    
    def validate_message(self, normalized_message: Dict[str, Any]) -> bool:
        """Validate normalized message"""
        try:
            if normalized_message['type'] == 'price_data':
                return self.validate_price_data(normalized_message)
            elif normalized_message['type'] == 'promo_data':
                return self.validate_promo_data(normalized_message)
            elif normalized_message['type'] == 'store_data':
                return self.validate_store_data(normalized_message)
            else:
                return True  # Generic data is always valid
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return False
    
    def validate_price_data(self, message: Dict[str, Any]) -> bool:
        """Validate price data"""
        required_fields = ['chain_id', 'store_id', 'items']
        for field in required_fields:
            if not message.get(field):
                logger.warning(f"Missing required field: {field}")
                return False
        
        # Validate items
        for item in message.get('items', []):
            if not item.get('item_code') or not item.get('item_name'):
                logger.warning("Item missing required fields: item_code or item_name")
                return False
        
        return True
    
    def validate_promo_data(self, message: Dict[str, Any]) -> bool:
        """Validate promotion data"""
        required_fields = ['chain_id', 'store_id', 'discounts']
        for field in required_fields:
            if not message.get(field):
                logger.warning(f"Missing required field: {field}")
                return False
        
        # Validate discounts
        for discount in message.get('discounts', []):
            if not discount.get('promotion_id') or not discount.get('item_code'):
                logger.warning("Discount missing required fields: promotion_id or item_code")
                return False
        
        return True
    
    def validate_store_data(self, message: Dict[str, Any]) -> bool:
        """Validate store data"""
        required_fields = ['chain_id', 'chain_name', 'stores']
        for field in required_fields:
            if not message.get(field):
                logger.warning(f"Missing required field: {field}")
                return False
        
        # Validate stores
        for store in message.get('stores', []):
            if not store.get('store_id') or not store.get('store_name'):
                logger.warning("Store missing required fields: store_id or store_name")
                return False
        
        return True
