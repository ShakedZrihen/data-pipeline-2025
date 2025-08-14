from datetime import datetime, timezone


def normalize(parsed_data, provider, branch, file_type):
    items = parsed_data.get('items', [])
    metadata = parsed_data.get('metadata', {})
    
    if 'promo' in file_type.lower():
        return _normalize_promotions(items, provider, branch, metadata)
    return _normalize_prices(items, provider, branch, metadata)

def _normalize_prices(items, provider, branch, metadata):
    
    normalized = []
    
    for item in items:
        try:
            normalized_item = {
                "barcode": item.get('ItemCode', ''),
                "product_name": item.get('ItemName', ''),
                "unit": _normalize_unit(item.get('UnitQty', '')),
                "quantity": item.get('Quantity', 0),
                "price": float(item.get('ItemPrice', 0.0)),
                "provider": provider,
                "chain_id": metadata.get('chainid', ''),
                "store_id": metadata.get('storeid', ''),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            if normalized_item['barcode']:
                normalized.append(normalized_item)
                
        except Exception as e:
            print(f"Error normalizing price item: {e}")
            continue
    
    return normalized

def safe_float(value, default=0.0):
    if value is None or value == '':
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default
    
def _normalize_promotions(items, provider, branch, metadata):

    normalized = []
    
    
    for item in items:
        try:
            normalized_item = {
                "promotion_id": item.get('PromotionId', ''),
                "description": item.get('PromotionDescription', ''),
                "start_date": item.get('PromotionStartDate', ''),
                "end_date": item.get('PromotionEndDate', ''),
                "barcode": item.get('ItemCode', ''),
                "min_quantity": safe_float(item.get('MinQty')),
                "max_quantity": safe_float(item.get('MaxQty')),
                "discounted_price": safe_float(item.get('DiscountedPrice')),
                "discount_rate": safe_float(item.get('DiscountRate')),
                "discount_type": item.get('DiscountType', ''),
                "reward_type": item.get('RewardType', ''),
                "provider": provider,
                "chain_id": metadata.get('chainid', ''),
                "store_id": metadata.get('storeid', ''),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            if normalized_item['promotion_id']:
                normalized.append(normalized_item)
                
        except Exception as e:
            print(f"Error normalizing promotion item: {e}")
            continue
    
    return normalized

def _normalize_unit(unit):
    if not unit:  
        return 'unit'
        
    unit_mapping = {
        'יח': 'unit',
        'יחידה': 'unit',
        'ק"ג': 'kg',
        'קג': 'kg',
        'גרם': 'gram',
        'ליטר': 'liter',
        'מ"ל': 'ml',
        'מטר': 'meter'
    }
    
    try:
        unit = str(unit).strip()
        return unit_mapping.get(unit, unit)
    except Exception as e:
        print(f"Failed to normalize unit '{unit}': {e}")
        return 'unit'
