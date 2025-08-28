import re
import xml.etree.ElementTree as ET
from decimal import Decimal



CURRENCY_PAT = r'(?:₪|ש["״”]?ח)\.?'

def extract_price_and_currency(text: str):
    t = text.replace("”", '"').replace("“", '"').replace("״", '"')

    cur_match = re.search(CURRENCY_PAT, t)
    currency = None
    cur_idx = len(t)
    if cur_match:
        currency_raw = cur_match.group(0)
        currency = "₪" if "₪" in currency_raw else "שח"  
        cur_idx = cur_match.start()

    m = re.search(r'(\d{1,3}[.,]\d{2})\s*' + (CURRENCY_PAT if cur_match else ""), t)
    if m and (not cur_match or m.start() < cur_idx):
        price = Decimal(m.group(1).replace(",", "."))
        return price, (currency or "שח")

    
    left = t[:cur_idx]
    nums = list(re.finditer(r'\d+', left))
    if len(nums) >= 2:
        last = nums[-1].group()   
        prev = nums[-2].group() 
        if len(prev) == 2 and 1 <= len(last) <= 3:
            price = Decimal(f"{int(last)}.{prev}")
            return price, (currency or "שח")

    if nums:
        price = Decimal(nums[-1].group())
        return price, (currency or "שח")

    return None, currency

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
            return parse_promotions(root)
        else:
            return parse_prices(root)
        

def parse_prices(root):
    metadata = extract_metadata(root)
    items = []
    
    try:
    
        for item in root.find('Items').findall('Item'):
            item_data = {
                'ItemCode': item.find('ItemCode').text if item.find('ItemCode') is not None else '',
                'ItemName': item.find('ItemName').text if item.find('ItemName') is not None else '',
                'UnitQty': item.find('UnitQty').text if item.find('UnitQty') is not None else '',
                'Quantity': int(float(item.find('Quantity').text)) if item.find('Quantity') is not None and item.find('Quantity').text else 0,
                'ItemPrice': float(item.find('ItemPrice').text) if item.find('ItemPrice') is not None and item.find('ItemPrice').text else 0.0,
                'ManufactureName': item.find('ManufacturerName').text if item.find('ManufacturerName') is not None else '',
            }
            items.append(item_data)
            
    except Exception as e:
        print(f"Failed to parse prices: {e}")

    return {
        'metadata': metadata,
        'items': items
    }

def parse_promotions(root):
    metadata = extract_metadata(root)
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
                        desc = sale.find('PromotionDescription').text if sale.find('PromotionDescription') is not None else ''
                        promo_price, promo_currency = extract_price_and_currency(desc)
                        promotion_data = {
                            'PromotionDescription': desc,
                            'PromotionStartDate': promo_start,
                            'PromotionEndDate': promo_end,
                            'PromoPrice': promo_price,
                            'PromoCurrency': promo_currency,
                            'MinQty': sale.find('MinQty').text if sale.find('MinQty') is not None else '',
                            'MaxQty': sale.find('MaxQty').text if sale.find('MaxQty') is not None else '',
                            'ItemCode': sale.find('ItemCode').text if sale.find('ItemCode') is not None else '',
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
                    desc = promotion.find('PromotionDescription').text if promotion.find('PromotionDescription') is not None else ''
                    promo_price, promo_currency = extract_price_and_currency(desc)
                    promotion_data = {
                        'PromotionDescription': desc,
                        'PromotionStartDate': promo_start_date,
                        'PromotionEndDate': promo_end_date,
                        'PromoPrice': promo_price,
                        'PromoCurrency': promo_currency,
                        'MinQty': promotion.find('MinQty').text if promotion.find('MinQty') is not None else '',
                        'MaxQty': promotion.find('MaxQty').text if promotion.find('MaxQty') is not None else '',
                        "Items": [
                            {
                                'ItemCode': item.find('ItemCode').text if item.find('ItemCode') is not None else '',
                                'ItemType': item.find('ItemType').text if item.find('ItemType') is not None else '',
                            }
                            for item in promotion.findall('PromotionItems/Item')
                        ]
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

def extract_metadata(root):
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
