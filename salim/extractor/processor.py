import re
import xml.etree.ElementTree as ET
from decimal import Decimal



CURRENCY_PAT = r'(?:₪|ש["״”]?ח)\.?'

def is_valid_barcode(code: str) -> bool:
    if not code:
        return False
    code_digits = re.sub(r'\D', '', code)
    return bool(re.fullmatch(r'\d{8,13}', code_digits))

def extract_price_and_currency(text: str):
    t = text.replace("”", '"').replace("“", '"').replace("״", '"')

    cur_match = re.search(CURRENCY_PAT, t)
    currency = None
    cur_idx = len(t)
    if cur_match:
        currency_raw = cur_match.group(0)
        currency = "ILS" if "₪" in currency_raw else ""  
        cur_idx = cur_match.start()

    m = re.search(r'(\d{1,3}[.,]\d{2})\s*' + (CURRENCY_PAT if cur_match else ""), t)
    if m and (not cur_match or m.start() < cur_idx):
        price = Decimal(m.group(1).replace(",", "."))
        return price, (currency or "ILS")

    
    left = t[:cur_idx]
    nums = list(re.finditer(r'\d+', left))
    if len(nums) >= 2:
        last = nums[-1].group()   
        prev = nums[-2].group() 
        if len(prev) == 2 and 1 <= len(last) <= 3:
            price = Decimal(f"{int(last)}.{prev}")
            return price, (currency or "ILS")

    if nums:
        price = Decimal(nums[-1].group())
        return price, (currency or "ILS")

    return None, currency

def parse(xml_content, file_type = 'pricesFull', provider=None, branch=None):
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            try:
                text = xml_content.decode('utf-8', errors='ignore')
                root = ET.fromstring(text)
            except Exception as e2:
                print(f"Failed to parse XML: {e2}")
                return {'items': [], 'metadata': {}}
        
        if file_type == 'promoFull' or file_type == 'promo':
            return parse_promotions(root, provider=provider, branch=branch)
        else:
            return parse_prices(root, provider=provider, branch=branch)


def parse_prices(root, provider=None, branch=None):
    metadata = extract_metadata(root)
    items = []
    
    try:
        items_elem = root.find('Items')
        if items_elem is None:
            items_elem = root.find('Products')
            items_data = items_elem.findall('Product') if items_elem is not None else []
        else:
            items_data = items_elem.findall('Item')
        if not items_data:
            print("No Items or Products element found in XML")
            return {
                'metadata': metadata,
                'items': []
            }

        for item in items_data:
            item_data = {
                'barcode': item.find('ItemCode').text if item.find('ItemCode') is not None else '',
                'canonical_name': item.find('ItemName').text if item.find('ItemName') is not None else '',
                'brand': '',
                'category': '',
                'size_value': float(item.find('Quantity').text) if item.find('Quantity') is not None and item.find('Quantity').text else None,
                'size_unit': item.find('UnitQty').text if item.find('UnitQty') is not None and item.find('UnitQty').text else '',
                'price': float(item.find('ItemPrice').text) if item.find('ItemPrice') is not None and item.find('ItemPrice').text else None,

            }
            items.append(item_data)
            
    except Exception as e:
        print(f"Failed to parse prices: {e}")

    return {
        'metadata': metadata,
        'items': items
    }

def parse_promotions(root, provider=None, branch=None):
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
                        desc = sale.find('PromotionDescription').text if sale.find('PromotionDescription') is not None else ''
                        promo_price, _ = extract_price_and_currency(desc)
                        promotion_data = {
                            'provider': provider,
                            'branch': branch,
                            'promo_text': desc,
                            'promo_price': float(promo_price) if promo_price is not None else None,
                            'items': [{
                                'barcode': sale.find('ItemCode').text if sale.find('ItemCode') is not None else '',
                            }]
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
                    items = []
                    invalid_item = None
                    for item in promotion.findall('PromotionItems/Item'):
                        itm_code = item.find('ItemCode').text if item.find('ItemCode') is not None else ''
                        itm_code = itm_code.strip() if itm_code else ''
                        if not is_valid_barcode(itm_code):
                            invalid_item = True
                        items.append({
                            'barcode': itm_code,
                        })

                    if invalid_item:
                        print(f"Warning: Invalid item found in promotion: {promotion.find('PromotionID').text if promotion.find('PromotionID') is not None else ''}")
                        continue

                    desc = promotion.find('PromotionDescription').text if promotion.find('PromotionDescription') is not None else ''
                    promo_price, _ = extract_price_and_currency(desc)
                    promotion_data = {
                        'provider': provider,
                        'branch': branch,
                        'promo_price': float(promo_price) if promo_price is not None else None,
                        'promo_text': desc,
                        'items': items
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
