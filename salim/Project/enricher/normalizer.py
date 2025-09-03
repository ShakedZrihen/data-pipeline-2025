import os
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class DataNormalizer:
    """Handles normalization of different data types into unified schema"""
    
    def normalize_store_id(self, store_id: str) -> str:
        """Normalize store ID by removing leading zeros"""
        if store_id and isinstance(store_id, str):
            return store_id.lstrip('0') or '0'  # Keep at least one zero if all zeros
        return store_id
    
    def normalize_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize message into unified schema"""
        try:
            data = message.get('data', {})
            metadata = message.get('metadata', {})
            file_type = metadata.get('file_type', '')
            
            # Extract data from Root if it exists
            root_data = data.get('Root', data)
            
            # Debug logging before fix
            logger.info(f"File type: {file_type}")
            logger.info(f"Original root_data keys: {list(root_data.keys()) if root_data else 'None'}")
            
            # Handle nested root structure (Root.root)
            if root_data and 'root' in root_data:
                logger.info(f"Found nested 'root' key, extracting...")
                root_data = root_data['root']
                logger.info(f"After extraction, root_data keys: {list(root_data.keys()) if root_data else 'None'}")
            
            logger.info(f"Final ChainId in root_data: {root_data.get('ChainId') if root_data else 'None'}")
            
            # Determine message type based on file_type in metadata
            if file_type == 'PriceFull':
                return self.normalize_price_data(root_data, metadata)
            elif file_type == 'PromoFull':
                return self.normalize_promo_data(root_data, metadata)  # PromoFull has different structure
            elif 'Stores' in str(data):
                return self.normalize_store_data(root_data, metadata)
            else:
                logger.info("Skipping generic data")
                return self.normalize_generic_data(data, metadata)
                
        except Exception as e:
            logger.error(f"Failed to normalize message: {e}")
            raise
    
    def normalize_price_data(self, data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize price/product data"""
        normalized = {
            'type': 'price_data',
            'chain_id': data.get('ChainId'),
            'store_id': self.normalize_store_id(data.get('StoreId', metadata.get('store_id', 'unknown'))),
            'items': []
        }
        
        # Extract items from the data structure
        items_data = data.get('Items', {}).get('Item', [])
        if not isinstance(items_data, list):
            items_data = [items_data]
        
        for item in items_data:
            normalized_item = {
                'item_code': item.get('ItemCode'),
                'item_id': item.get('ItemId'),
                'item_type': item.get('ItemType'),
                'item_name': item.get('ItemName'),
                'manufacturer_name': item.get('ManufacturerName'),
                'manufacture_country': item.get('ManufactureCountry'),
                'manufacturer_item_description': item.get('ManufacturerItemDescription'),
                'unit_qty': item.get('UnitQty'),
                'quantity': float(item.get('Quantity', 0)),
                'unit_of_measure': item.get('UnitOfMeasure'),
                'is_weighted': bool(int(item.get('bIsWeighted', 0))),
                'qty_in_package': item.get('QtyInPackage'),
                'item_price': float(item.get('ItemPrice', 0)),
                'unit_of_measure_price': float(item.get('UnitOfMeasurePrice', 0)),
                'allow_discount': bool(int(item.get('AllowDiscount', 1))),
                'item_status': int(item.get('ItemStatus', 1)),
                'item_brand': item.get('ItemBrand'),  # Will be enriched later
                'price_update_date': item.get('PriceUpdateDate')
            }
            normalized['items'].append(normalized_item)
        
        return normalized
    
    def normalize_promo_data(self, data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize promotion data"""
        normalized = {
            'type': 'promo_data',
            'chain_id': data.get('ChainId'),
            'store_id': self.normalize_store_id(data.get('StoreId', metadata.get('store_id', 'unknown'))),
            'discounts': []
        }
        
        # Extract promotions from the data structure
        promotions_data = data.get('Promotions', {}).get('Promotion', [])
        if not isinstance(promotions_data, list):
            promotions_data = [promotions_data]
        
        logger.info(f"Processing {len(promotions_data)} promotions")
        
        # Check if we're in test mode to limit processing
        test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
        max_promotions = 2 if test_mode else len(promotions_data)
        
        for promo_idx, promotion in enumerate(promotions_data[:max_promotions]):
            # Extract promotion items
            promo_items = promotion.get('PromotionItems', {}).get('Item', [])
            if not isinstance(promo_items, list):
                promo_items = [promo_items]
            
            # Limit items in test mode too
            max_items = 5 if test_mode else len(promo_items)
            logger.info(f"Promotion {promo_idx + 1}/{max_promotions}: Processing {min(len(promo_items), max_items)} items out of {len(promo_items)} total")
            
            # Extract additional restrictions
            additional = promotion.get('AdditionalRestrictions', {})
            
            # Extract club info
            clubs = promotion.get('Clubs', {})
            club_id = clubs.get('ClubId') if isinstance(clubs, dict) else None
            
            for item_idx, item in enumerate(promo_items[:max_items]):
                normalized_discount = {
                    'promotion_id': promotion.get('PromotionId'),
                    'promotion_description': promotion.get('PromotionDescription'),
                    'promotion_update_date': promotion.get('PromotionUpdateDate'),
                    'promotion_start_date': promotion.get('PromotionStartDate'),
                    'promotion_start_hour': promotion.get('PromotionStartHour'),
                    'promotion_end_date': promotion.get('PromotionEndDate'),
                    'promotion_end_hour': promotion.get('PromotionEndHour'),
                    'reward_type': int(promotion.get('RewardType', 0)),
                    'allow_multiple_discounts': bool(int(promotion.get('AllowMultipleDiscounts', 0))),
                    'is_weighted_promo': bool(int(promotion.get('IsWeightedPromo', 0))),
                    'min_qty': float(promotion.get('MinQty', 0)),
                    'discounted_price': float(promotion.get('DiscountedPrice', 0)),
                    'discounted_price_per_mida': float(promotion.get('DiscountedPricePerMida', 0)),
                    'min_no_of_item_offered': int(promotion.get('MinNoOfItemOfered', 0)),
                    'item_code': item.get('ItemCode'),
                    'item_type': item.get('ItemType'),
                    'is_gift_item': bool(int(item.get('IsGiftItem', 0))),
                    'club_id': club_id,
                    'additional_is_coupon': bool(int(additional.get('AdditionalIsCoupon', 0))),
                    'additional_gift_count': int(additional.get('AdditionalGiftCount', 0)),
                    'additional_is_total': bool(int(additional.get('AdditionalIsTotal', 0))),
                    'additional_is_active': bool(int(additional.get('AdditionalIsActive', 1)))
                }
                normalized['discounts'].append(normalized_discount)
        
        return normalized
    
    def normalize_store_data(self, data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize store data"""
        normalized = {
            'type': 'store_data',
            'chain_id': data.get('ChainId'),  # Fixed: was ChainID, should be ChainId
            'chain_name': data.get('ChainName'),
            'stores': []
        }
        
        # Extract stores from the data structure
        sub_chains = data.get('SubChains', {}).get('SubChain', [])
        if not isinstance(sub_chains, list):
            sub_chains = [sub_chains]
        
        for sub_chain in sub_chains:
            stores_data = sub_chain.get('Stores', {}).get('Store', [])
            if not isinstance(stores_data, list):
                stores_data = [stores_data]
            
            for store in stores_data:
                normalized_store = {
                    'sub_chain_id': sub_chain.get('SubChainID'),
                    'sub_chain_name': sub_chain.get('SubChainName'),
                    'store_id': self.normalize_store_id(store.get('StoreID')),
                    'bikoret_no': store.get('BikoretNo'),
                    'store_type': int(store.get('StoreType', 0)),
                    'store_name': store.get('StoreName'),
                    'address': store.get('Address'),
                    'city': store.get('City'),
                    'zip_code': store.get('ZipCode'),
                    'last_update_date': data.get('LastUpdateDate'),
                    'last_update_time': data.get('LastUpdateTime')
                }
                normalized['stores'].append(normalized_store)
        
        return normalized
    
    def normalize_generic_data(self, data: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize generic data"""
        return {
            'type': 'generic_data',
            'data': data,
            'metadata': metadata
        }
