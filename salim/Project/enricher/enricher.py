import os
import logging
from datetime import datetime
from typing import Dict, Any
from claude_brand_extractor import ClaudeBrandExtractor

logger = logging.getLogger(__name__)

class DataEnricher:
    """Handles enrichment of normalized data with additional fields"""
    
    def __init__(self):
        self.brand_extractor = ClaudeBrandExtractor()
        self.test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
    
    def enrich_message(self, normalized_message: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich message with additional data"""
        try:
            if normalized_message['type'] == 'price_data':
                return self.enrich_price_data(normalized_message)
            elif normalized_message['type'] == 'promo_data':
                return self.enrich_promo_data(normalized_message)
            elif normalized_message['type'] == 'store_data':
                return self.enrich_store_data(normalized_message)
            else:
                return normalized_message
        except Exception as e:
            logger.error(f"Failed to enrich message: {e}")
            return normalized_message
    
    def enrich_price_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich price data with additional fields"""
        # Add timestamp if missing
        if not message.get('processed_at'):
            message['processed_at'] = datetime.now().isoformat()
        
        items = message.get('items', [])
        logger.info(f"Processing {len(items)} items for enrichment")
        
        fast_test_mode = os.getenv('FAST_TEST_MODE', 'false').lower() == 'true'
        if self.test_mode or fast_test_mode:
            if fast_test_mode:
                max_items = int(os.getenv('FAST_TEST_ITEM_LIMIT', '10'))
            else:
                max_items = 20  # Regular test mode
            items_to_process = items[:max_items] if len(items) > max_items else items
            logger.info(f"{'FAST TEST' if fast_test_mode else 'TEST'} MODE: Processing only first {len(items_to_process)} items out of {len(items)} total")
            message['items'] = items_to_process
        else:
            items_to_process = items
        
        for i, item in enumerate(items_to_process):
            if (i + 1) % 10 == 0:  # Log progress every 10 items
                logger.info(f"Processed {i + 1}/{len(items_to_process)} items")
            
            if not item.get('item_promotion'):
                item['item_promotion'] = False
            if not item.get('item_promotion_price'):
                item['item_promotion_price'] = 0.0
            
            if not item.get('item_brand'):
                try:
                    item_for_extraction = {
                        'item_name': item.get('item_name', ''),
                        'manufacturer': item.get('manufacturer_name', ''),
                        'description': item.get('manufacturer_item_description', '')
                    }
                    enriched_item = self.brand_extractor.enrich_item_with_brand(item_for_extraction)
                    
                    # Update the original item with brand info
                    item['item_brand'] = enriched_item.get('item_brand', 'Unknown')
                    item['brand_confidence'] = enriched_item.get('brand_confidence', 0.0)
                    item['brand_extraction_method'] = enriched_item.get('brand_extraction_method', 'unknown')
                    
                    if item.get('brand_confidence', 0) >= 0.7:
                        logger.info(f"Extracted brand '{item.get('item_brand')}' for item '{item.get('item_name', 'unknown')}' "
                                   f"(confidence: {item.get('brand_confidence', 0):.2f})")
                    elif item.get('brand_confidence', 0) >= 0.4:
                        logger.info(f"Extracted brand '{item.get('item_brand')}' for item '{item.get('item_name', 'unknown')}' "
                                   f"(confidence: {item.get('brand_confidence', 0):.2f}) - {item.get('brand_extraction_method', 'unknown')}")
                except Exception as e:
                    logger.warning(f"Brand extraction failed for item '{item.get('item_name', 'unknown')}': {e}")
                    # Set default values to continue processing
                    item['item_brand'] = 'Unknown'
                    item['brand_confidence'] = 0.0
                    item['brand_extraction_method'] = 'extraction_failed'
        
        return message
    
    def enrich_promo_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich promotion data with additional fields"""
        # Add timestamp if missing
        if not message.get('processed_at'):
            message['processed_at'] = datetime.now().isoformat()
        
        # Enrich discounts with default values
        for discount in message.get('discounts', []):
            if discount.get('allow_multiple_discounts') is None:
                discount['allow_multiple_discounts'] = False
            if discount.get('min_qty') is None:
                discount['min_qty'] = 0.0
            if discount.get('is_weighted_promo') is None:
                discount['is_weighted_promo'] = False
            if discount.get('is_gift_item') is None:
                discount['is_gift_item'] = False
        
        return message
    
    def enrich_store_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich store data with additional fields"""
        # Add timestamp if missing
        if not message.get('processed_at'):
            message['processed_at'] = datetime.now().isoformat()
        
        # Enrich stores with default values
        for store in message.get('stores', []):
            if not store.get('store_type'):
                store['store_type'] = 1
            if not store.get('city'):
                store['city'] = 'Unknown'
        
        return message
