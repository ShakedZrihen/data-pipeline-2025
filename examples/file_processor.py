import gzip
import xml.etree.ElementTree as ET
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)

class FileProcessor:
    
    def __init__(self):
        self.supported_extensions = ['.gz']
    
    def can_process(self, file_path: str) -> bool:
        """Check if file can be processed"""
        return Path(file_path).suffix in self.supported_extensions
    
    def decompress_file(self, file_path: str) -> str:
        """Decompress .gz file and return content as string"""
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                content = f.read()
            logger.info(f"Successfully decompressed {file_path}")
            return content
        except Exception as e:
            logger.error(f"Failed to decompress {file_path}: {e}")
            raise
    
    def parse_xml(self, xml_content: str) -> ET.Element:
        try:
            root = ET.fromstring(xml_content)
            logger.info("Successfully parsed XML content")
            return root
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML: {e}")
            raise
    
    def extract_metadata(self, root: ET.Element) -> Dict:
        metadata = {}
        
        # Extract basic metadata
        for tag in ['XmlDocVersion', 'ChainId', 'SubChainId', 'StoreId', 'BikoretNo', 'DllVerNo']:
            elem = root.find(tag)
            if elem is not None:
                metadata[tag] = elem.text
        
        # Determine file type
        if root.find('Items') is not None:
            metadata['type'] = 'pricesFull'
            metadata['items_count'] = root.find('Items').get('Count', '0')
        elif root.find('Promotions') is not None:
            metadata['type'] = 'promoFull'
            metadata['promotions_count'] = root.find('Promotions').get('Count', '0')
        
        return metadata
    
    def extract_items(self, root: ET.Element) -> List[Dict]:
        """Extract items from price files"""
        items = []
        items_elem = root.find('Items')
        
        if items_elem is None:
            return items
        
        for item_elem in items_elem.findall('Item'):
            item = {}
            for elem in item_elem:
                item[elem.tag] = elem.text
            
            # Normalize item data
            normalized_item = {
                'product': item.get('ItemName', ''),
                'price': float(item.get('ItemPrice', 0)),
                'unit_price': float(item.get('UnitOfMeasurePrice', 0)),
                'quantity': item.get('Quantity', ''),
                'unit': item.get('UnitOfMeasure', ''),
                'item_code': item.get('ItemCode', ''),
                'manufacturer': item.get('ManufacturerName', ''),
                'update_date': item.get('PriceUpdateDate', '')
            }
            items.append(normalized_item)
        
        return items
    
    def extract_promotions(self, root: ET.Element) -> List[Dict]:
        """Extract promotions from promo files"""
        promotions = []
        promos_elem = root.find('Promotions')
        
        if promos_elem is None:
            return promotions
        
        for promo_elem in promos_elem.findall('Promotion'):
            promo = {}
            for elem in promo_elem:
                promo[elem.tag] = elem.text
            
            # Normalize promotion data
            normalized_promo = {
                'promotion_id': promo.get('PromotionId', ''),
                'description': promo.get('PromotionDescription', ''),
                'start_date': promo.get('PromotionStartDate', ''),
                'end_date': promo.get('PromotionEndDate', ''),
                'discount_type': promo.get('DiscountType', ''),
                'discount_value': promo.get('DiscountValue', '')
            }
            promotions.append(normalized_promo)
        
        return promotions
    
    def process_file(self, file_path: str) -> Tuple[Dict, List]:
        """Main method to process a file and return metadata and items/promotions"""
        if not self.can_process(file_path):
            raise ValueError(f"Unsupported file type: {file_path}")
        
        # Decompress and parse
        xml_content = self.decompress_file(file_path)
        root = self.parse_xml(xml_content)
        
        # Extract metadata
        metadata = self.extract_metadata(root)
        
        # Extract content based on type
        if metadata.get('type') == 'pricesFull':
            content = self.extract_items(root)
        elif metadata.get('type') == 'promoFull':
            content = self.extract_promotions(root)
        else:
            content = []
        
        return metadata, content
