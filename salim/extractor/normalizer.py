from datetime import datetime, timezone


def normalize(parsed_data, provider, file_type):
    items = parsed_data.get('items', [])
    metadata = parsed_data.get('metadata', {})
    
    if 'promo' in file_type.lower():
        return normalize_promotions(items, provider, metadata)
    return normalize_prices(items, provider, metadata)

def normalize_prices(items, provider, metadata):
    
    normalized = []
    
    for item in items:
        try:
            normalized_item = {
                "barcode": item.get('ItemCode', ''),
                "product_name": item.get('ItemName', ''),
                "price": float(item.get('ItemPrice', 0.0)),
                "provider": provider,
                "branch_id": metadata.get('storeid', ''),
                "manufacture_name": item.get('ManufactureName', ''),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            if normalized_item['barcode'] and normalized_item['price'] > 0:
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
    
def normalize_promotions(items, provider, metadata):
    normalized = []
    
    for item in items:
        try:
            base_promo = {
                "description": item.get('PromotionDescription', ''),
                "start_date": item.get('PromotionStartDate', ''),
                "end_date": item.get('PromotionEndDate', ''),
                "promo_price": safe_float(item.get('PromoPrice')),
                "promo_currency": item.get('PromoCurrency', ''),
                "min_quantity": safe_float(item.get('MinQty')),
                "max_quantity": safe_float(item.get('MaxQty')),
                "provider": provider,
                "branch_id": metadata.get('storeid', ''),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            items_list = item.get('Items', [])
            if isinstance(items_list, list):
                for sub_item in items_list:
                    promo_item = base_promo.copy()
                    promo_item.update({
                        "barcode": sub_item.get('ItemCode', ''),
                    })
                    
                    if promo_item['barcode']:
                        normalized.append(promo_item)
            else:
                base_promo["barcode"] = item.get('ItemCode', '')
                if base_promo['barcode']:
                    normalized.append(base_promo)
                    
        except Exception as e:
            print(f"Error normalizing promotion item: {e}")
            continue
    
    print(f"Normalized {len(normalized)} promotion items")
    return normalized
