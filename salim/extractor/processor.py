import xml.etree.ElementTree as ET



def parse(xml_content, file_type = 'pricesFull'):
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            try:
                text = xml_content.decode('utf-8', errors='ignore')
                root = ET.fromstring(text)
            except Exception as e2:
                print(f"Failed to parse XML: {e2}")
                return {'items': [], 'metadata': {}}
        
        if file_type == 'promoFull':
            return _parse_promotions(root)
        else:
            return _parse_prices(root)
        

def _parse_prices(root):
    metadata = _extract_metadata(root)
    items = []
    
    try:
    
        for item in root.find('Items').findall('Item'):
            item_data = {
                'ItemCode': item.find('ItemCode').text if item.find('ItemCode') is not None else '',
                'ItemName': item.find('ItemName').text if item.find('ItemName') is not None else '',
                'UnitQty': item.find('UnitQty').text if item.find('UnitQty') is not None else '',
                'Quantity': int(float(item.find('Quantity').text)) if item.find('Quantity') is not None and item.find('Quantity').text else 0,
                'ItemPrice': float(item.find('ItemPrice').text) if item.find('ItemPrice') is not None and item.find('ItemPrice').text else 0.0
            }
            items.append(item_data)
            
    except Exception as e:
        print(f"Failed to parse prices: {e}")

    return {
        'metadata': metadata,
        'items': items
    }

def _parse_promotions(root):
    metadata = _extract_metadata(root)
    promotions = []
    
    try:
        promotions_elem = root.find('Promotions')
        if promotions_elem is None:
            print("Looking for Sales structure...")
            sales_elem = root.find('Sales')
            
            if sales_elem is not None:
                print("Found Sales element, processing Sale items...")
                for sale in sales_elem.findall('Sale'):
                    try:
                        promo_start = f"{sale.find('PromotionStartDate').text if sale.find('PromotionStartDate') is not None else ''} {sale.find('PromotionStartHour').text if sale.find('PromotionStartHour') is not None else ''}"
                        promo_end = f"{sale.find('PromotionEndDate').text if sale.find('PromotionEndDate') is not None else ''} {sale.find('PromotionEndHour').text if sale.find('PromotionEndHour') is not None else ''}"
                        
                        promotion_data = {
                            'PromotionId': sale.find('PromotionID').text if sale.find('PromotionID') is not None else '',
                            'PromotionDescription': sale.find('PromotionDescription').text if sale.find('PromotionDescription') is not None else '',
                            'PromotionStartDate': promo_start,
                            'PromotionEndDate': promo_end,
                            'RewardType': sale.find('RewardType').text if sale.find('RewardType') is not None else '',
                            'AllowMultipleDiscounts': sale.find('AllowMultipleDiscounts').text if sale.find('AllowMultipleDiscounts') is not None else '',
                            'IsWeightedPromo': '0',
                            'MinQty': sale.find('MinQty').text if sale.find('MinQty') is not None else '',
                            'MaxQty': sale.find('MaxQty').text if sale.find('MaxQty') is not None else '',
                            'DiscountedPrice': sale.find('DiscountedPrice').text if sale.find('DiscountedPrice') is not None else '',
                            'DiscountedPricePerMida': sale.find('DiscountedPricePerMida').text if sale.find('DiscountedPricePerMida') is not None else '',
                            'ItemCode': sale.find('ItemCode').text if sale.find('ItemCode') is not None else '',
                            'PriceUpdateDate': sale.find('PriceUpdateDate').text if sale.find('PriceUpdateDate') is not None else '',
                            'DiscountRate': float(sale.find('DiscountRate').text) if sale.find('DiscountRate') is not None and sale.find('DiscountRate').text else 0.0,
                            'DiscountType': sale.find('DiscountType').text if sale.find('DiscountType') is not None else '',
                            'Remarks': sale.find('Remarks').text if sale.find('Remarks') is not None else ''
                        }
                        promotions.append(promotion_data)
                        
                    except Exception as sale_error:
                        print(f"Warning: Failed to parse sale item: {sale_error}")
                        continue
            else:
                print("No promotions or sales data found in XML")
                
        else:

            for promotion in promotions_elem.findall('Promotion'):
                try:
                    promo_start_date = f"{promotion.find('PromotionStartDate').text if promotion.find('PromotionStartDate') is not None else ''} {promotion.find('PromotionStartHour').text if promotion.find('PromotionStartHour') is not None else ''}"
                    promo_end_date = f"{promotion.find('PromotionEndDate').text if promotion.find('PromotionEndDate') is not None else ''} {promotion.find('PromotionEndHour').text if promotion.find('PromotionEndHour') is not None else ''}"
                    promotion_data = {
                        'PromotionId': promotion.find('PromotionID').text if promotion.find('PromotionID') is not None else '',
                        'PromotionDescription': promotion.find('PromotionDescription').text if promotion.find('PromotionDescription') is not None else '',
                        'PromotionStartDate': promo_start_date,
                        'PromotionEndDate': promo_end_date,
                        'IsWeightedPromo': promotion.find('IsWeightedPromo').text if promotion.find('IsWeightedPromo') is not None else '',
                        'MinQty': promotion.find('MinQty').text if promotion.find('MinQty') is not None else '',
                        'MaxQty': promotion.find('MaxQty').text if promotion.find('MaxQty') is not None else '',
                        'ItemCode': promotion.find('.//Item/ItemCode').text if promotion.find('.//Item/ItemCode') is not None else ''
                    }
                    promotions.append(promotion_data)
                except Exception as item_error:
                    print(f"Warning: Failed to parse promotion item: {item_error}")
                    continue
                
    except Exception as e:
        print(f"Failed to parse promotions: {e}")

    return {
        'metadata': metadata,
        'items': promotions
    }

def _extract_metadata(root):
    metadata = {}
    
    metadata_fields = [
        'ChainId', 'ChainID', 'SubChainId', 'SubChainID',
        'StoreId', 'StoreID', 'BikoretNo'
    ]
    
    for field in metadata_fields:
        elem = root.find(field)
        if elem is not None and elem.text:
            metadata[field.lower()] = elem.text.strip()
    
    return metadata
