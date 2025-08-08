"""
XML Processor for Israeli Supermarket Price/Promo Files
Handles different XML schemas from various providers
"""

import xml.etree.ElementTree as ET
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class XMLProcessor:
    """
    Process XML files from different Israeli supermarket chains.
    Each chain has its own XML schema, so we need provider-specific parsing.
    """
    
    def __init__(self):
        self.parsers = {
            'victory': self._parse_victory_format,
            'carrefour': self._parse_carrefour_format,
            'yohananof': self._parse_yohananof_format,
            'default': self._parse_generic_format
        }
    
    def parse(self, xml_content: bytes, provider: str = 'default', file_type: str = 'pricesFull') -> Dict[str, Any]:
        """
        Parse XML content based on provider and file type.
        
        Args:
            xml_content: Raw XML bytes
            provider: Provider name (victory, carrefour, yohananof, etc.)
            file_type: Either 'pricesFull' or 'promoFull'
            
        Returns:
            Dictionary with parsed data
        """
        try:
            # Parse XML
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            # Try with UTF-8 decoding
            try:
                text = xml_content.decode('utf-8', errors='ignore')
                root = ET.fromstring(text)
            except Exception as e2:
                logger.error(f"Failed to parse XML: {e2}")
                return {'items': [], 'metadata': {}}
        
        # Get parser for provider
        parser_func = self.parsers.get(provider.lower(), self.parsers['default'])
        
        # Parse based on file type
        if file_type == 'promoFull':
            return self._parse_promotions(root, parser_func)
        else:
            return self._parse_prices(root, parser_func)
    
    def _parse_prices(self, root: ET.Element, parser_func) -> Dict[str, Any]:
        """Parse price files"""
        metadata = self._extract_metadata(root)
        items = parser_func(root, 'prices')
        return {
            'metadata': metadata,
            'items': items
        }
    
    def _parse_promotions(self, root: ET.Element, parser_func) -> Dict[str, Any]:
        """Parse promotion files"""
        metadata = self._extract_metadata(root)
        items = parser_func(root, 'promotions')
        return {
            'metadata': metadata,
            'items': items
        }
    
    def _extract_metadata(self, root: ET.Element) -> Dict[str, Any]:
        """Extract metadata from XML root"""
        metadata = {}
        
        # Common metadata fields
        metadata_fields = [
            'ChainId', 'ChainID', 'SubChainId', 'SubChainID',
            'StoreId', 'StoreID', 'BikoretNo'
        ]
        
        for field in metadata_fields:
            elem = root.find(field)
            if elem is not None and elem.text:
                metadata[field.lower()] = elem.text.strip()
        
        return metadata
    
    def _parse_victory_format(self, root: ET.Element, data_type: str) -> List[Dict[str, Any]]:
        """
        Parse Victory/Carrefour format XML
        Example structure:
        <Prices>
            <Products>
                <Product>
                    <ItemCode>...</ItemCode>
                    <ItemName>...</ItemName>
                    <ItemPrice>...</ItemPrice>
                    ...
                </Product>
            </Products>
        </Prices>
        """
        items = []
        
        if data_type == 'prices':
            # Find Products container
            products_elem = root.find('Products')
            if products_elem is None:
                products_elem = root  # Sometimes products are direct children
            
            # Parse each product
            for product in products_elem.findall('Product'):
                item = {}
                
                # Map fields
                field_mapping = {
                    'ItemCode': 'barcode',
                    'ItemName': 'name',
                    'ItemPrice': 'price',
                    'ManufactureName': 'manufacturer',
                    'ManufacturerName': 'manufacturer',
                    'Quantity': 'quantity',
                    'UnitQty': 'unit',
                    'UnitMeasure': 'unit_measure'
                }
                
                for xml_field, json_field in field_mapping.items():
                    elem = product.find(xml_field)
                    if elem is not None and elem.text:
                        value = elem.text.strip()
                        if json_field == 'price':
                            try:
                                value = float(value.replace(',', ''))
                            except ValueError:
                                value = None
                        item[json_field] = value
                
                if item:
                    items.append(item)
        
        elif data_type == 'promotions':
            # Parse promotions
            promotions_elem = root.find('Promotions')
            if promotions_elem is None:
                promotions_elem = root
            
            for promo in promotions_elem.findall('Promotion'):
                # Get promotion details
                promo_data = {
                    'promotion_id': self._get_text(promo, 'PromotionId'),
                    'description': self._get_text(promo, 'PromotionDescription'),
                    'start_date': self._get_text(promo, 'PromotionStartDate'),
                    'end_date': self._get_text(promo, 'PromotionEndDate'),
                    'discount_price': self._get_float(promo, 'DiscountedPrice'),
                    'min_qty': self._get_float(promo, 'MinQty')
                }
                
                # Get items in promotion
                promo_items = promo.find('PromotionItems')
                if promo_items is not None:
                    for item_elem in promo_items.findall('Item'):
                        item = promo_data.copy()
                        item['barcode'] = self._get_text(item_elem, 'ItemCode')
                        items.append(item)
        
        return items
    
    def _parse_carrefour_format(self, root: ET.Element, data_type: str) -> List[Dict[str, Any]]:
        """Parse Carrefour format (similar to Victory)"""
        # Carrefour uses similar format to Victory
        return self._parse_victory_format(root, data_type)
    
    def _parse_yohananof_format(self, root: ET.Element, data_type: str) -> List[Dict[str, Any]]:
        """
        Parse Yohananof format XML
        Might have different structure
        """
        # Try Victory format first as a fallback
        items = self._parse_victory_format(root, data_type)
        
        # If no items found, try alternative structure
        if not items:
            items = self._parse_generic_format(root, data_type)
        
        return items
    
    def _parse_generic_format(self, root: ET.Element, data_type: str) -> List[Dict[str, Any]]:
        """
        Generic parser that tries to find product-like elements
        """
        items = []
        
        # Look for any element that might be a product/item
        possible_product_tags = ['Product', 'product', 'Item', 'item', 'ProductItem']
        
        for tag in possible_product_tags:
            for elem in root.iter(tag):
                item = {}
                
                # Try to extract common fields
                for child in elem:
                    tag_lower = child.tag.lower()
                    text = (child.text or '').strip()
                    
                    if not text:
                        continue
                    
                    # Map common field patterns
                    if 'name' in tag_lower or 'description' in tag_lower:
                        item['name'] = text
                    elif 'price' in tag_lower:
                        try:
                            item['price'] = float(text.replace(',', ''))
                        except ValueError:
                            pass
                    elif 'code' in tag_lower or 'barcode' in tag_lower:
                        item['barcode'] = text
                    elif 'manufacturer' in tag_lower or 'brand' in tag_lower:
                        item['manufacturer'] = text
                    elif 'quantity' in tag_lower or 'qty' in tag_lower:
                        item['quantity'] = text
                    elif 'unit' in tag_lower:
                        item['unit'] = text
                
                if item:
                    items.append(item)
        
        return items
    
    def _get_text(self, elem: ET.Element, tag: str) -> Optional[str]:
        """Safely get text from child element"""
        child = elem.find(tag)
        if child is not None and child.text:
            return child.text.strip()
        return None
    
    def _get_float(self, elem: ET.Element, tag: str) -> Optional[float]:
        """Safely get float value from child element"""
        text = self._get_text(elem, tag)
        if text:
            try:
                return float(text.replace(',', ''))
            except ValueError:
                pass
        return None
