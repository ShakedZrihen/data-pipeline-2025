import gzip
import logging
import xml.etree.ElementTree as ET
from typing import Dict, Any, Optional, List
from io import BytesIO

logger = logging.getLogger(__name__)


class FileProcessor:
    """Process compressed files and extract XML data"""
    
    def __init__(self):
        pass
    
    def process_file(self, file_content: BytesIO, file_metadata: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """
        Process compressed file and extract data
        
        Args:
            file_content: BytesIO object with file content
            file_metadata: File metadata dictionary
            
        Returns:
            Processed data dictionary or None if failed
        """
        try:
            print(f"[INFO] Processing file: {file_metadata.get('filename', 'unknown')}")
            
            # Decompress the gzipped content
            file_content.seek(0)
            decompressed_data = self._decompress_file(file_content)
            
            if not decompressed_data:
                print(f"[ERROR] Failed to decompress file")
                return None
            
            # Parse XML content
            parsed_data = self._parse_xml_content(decompressed_data, file_metadata)
            
            if not parsed_data:
                print(f"[ERROR] Failed to parse XML content")
                return None
            
            print(f"[SUCCESS] Successfully processed file with {len(parsed_data.get('items', []))} items")
            return parsed_data
            
        except Exception as e:
            print(f"[ERROR] Error processing file {file_metadata.get('filename', 'unknown')}: {str(e)}")
            return None
    
    def _decompress_file(self, file_content: BytesIO) -> Optional[str]:
        """
        Decompress gzipped file content
        
        Args:
            file_content: BytesIO object with gzipped content
            
        Returns:
            Decompressed string content or None if failed
        """
        try:
            file_content.seek(0)
            with gzip.GzipFile(fileobj=file_content, mode='rb') as gz_file:
                decompressed_data = gz_file.read()
                return decompressed_data.decode('utf-8')
        except Exception as e:
            print(f"[ERROR] Error decompressing file: {str(e)}")
            return None
    
    def _parse_xml_content(self, xml_content: str, file_metadata: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """
        Parse XML content and extract product data
        
        Args:
            xml_content: Decompressed XML content
            file_metadata: File metadata dictionary
            
        Returns:
            Parsed data dictionary or None if failed
        """
        try:
            root = ET.fromstring(xml_content)
            
            file_type = file_metadata.get('type', '')
            
            if file_type == 'pricesFull':
                items = self._parse_prices_xml(root)
            elif file_type == 'promoFull':
                items = self._parse_promos_xml(root)
            else:
                print(f"[ERROR] Unknown file type: {file_type}")
                return None
            
            return {
                'provider': file_metadata.get('provider', ''),
                'branch': file_metadata.get('branch', ''),
                'type': file_type,
                'timestamp': file_metadata.get('timestamp', ''),
                'items': items,
                'total_items': len(items)
            }
            
        except ET.ParseError as e:
            print(f"[ERROR] XML parsing error: {str(e)}")
            return None
        except Exception as e:
            print(f"[ERROR] Error parsing XML content: {str(e)}")
            return None
    
    def _parse_prices_xml(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        Parse price data from XML
        
        Args:
            root: XML root element
            
        Returns:
            List of price items
        """
        items = []
        
        # Parse Israeli supermarket price XML structure
        price_elements = root.findall('.//Item')
        
        for item in price_elements:
            try:
                # Extract price data from the actual XML structure
                product_name = self._get_xml_text(item, ['ItemName'])
                price = self._get_xml_text(item, ['ItemPrice'])
                unit_price = self._get_xml_text(item, ['UnitOfMeasurePrice'])
                unit = self._get_xml_text(item, ['UnitOfMeasure', 'UnitQty'])
                manufacturer = self._get_xml_text(item, ['ManufacturerName'])
                item_code = self._get_xml_text(item, ['ItemCode'])
                
                if product_name and price:
                    # Clean up unit text (remove extra spaces)
                    clean_unit = unit.strip() if unit else 'unit'
                    
                    item_data = {
                        'product': product_name,
                        'price': self._parse_price(price),
                        'unit': self._normalize_unit_hebrew(clean_unit)
                    }
                    
                    # Add optional fields
                    if unit_price and unit_price != price:
                        item_data['unit_price'] = self._parse_price(unit_price)
                    if manufacturer:
                        item_data['manufacturer'] = manufacturer
                    if item_code:
                        item_data['item_code'] = item_code
                    
                    items.append(item_data)
                    
            except Exception as e:
                print(f"[WARNING] Error parsing price item: {str(e)}")
                continue
        
        return items
    
    def _normalize_unit_hebrew(self, unit: str) -> str:
        """
        Normalize Hebrew unit names to standard format
        
        Args:
            unit: Raw unit string in Hebrew
            
        Returns:
            Normalized unit
        """
        if not unit:
            return "unit"
        
        unit_lower = unit.lower().strip()
        
        # Hebrew unit normalizations
        hebrew_unit_mappings = {
            'ליטר': 'liter',
            'ליטרים': 'liter', 
            'מיליליטר': 'milliliter',
            'מיליליטרים': 'milliliter',
            'מ"ל': 'milliliter',
            'גרם': 'gram',
            'גרמים': 'gram',
            'ג': 'gram',
            'קילו': 'kilogram',
            'קילוגרם': 'kilogram',
            'קילוגרמים': 'kilogram',
            'קג': 'kilogram',
            'יחידה': 'unit',
            'יח': 'unit',
            'חבילה': 'package',
            'בקבוק': 'bottle',
            'שקית': 'bag'
        }
        
        return hebrew_unit_mappings.get(unit_lower, unit_lower)
    
    def _parse_promos_xml(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        Parse promotion data from XML
        
        Args:
            root: XML root element
            
        Returns:
            List of promotion items
        """
        items = []
        
        # Parse Israeli supermarket promotion XML structure
        promo_elements = root.findall('.//Promotion')
        
        for promo in promo_elements:
            try:
                # Extract promotion data from the actual XML structure
                product_name = self._get_xml_text(promo, ['PromotionDescription'])
                discounted_price = self._get_xml_text(promo, ['DiscountedPrice'])
                discounted_price_per_mida = self._get_xml_text(promo, ['DiscountedPricePerMida'])
                min_qty = self._get_xml_text(promo, ['MinQty'])
                
                if product_name and discounted_price:
                    items.append({
                        'product': product_name,
                        'price': self._parse_price(discounted_price),
                        'price_per_unit': self._parse_price(discounted_price_per_mida) if discounted_price_per_mida else None,
                        'min_quantity': self._parse_price(min_qty) if min_qty else None,
                        'unit': 'promotion'
                    })
                    
            except Exception as e:
                print(f"[WARNING] Error parsing promo item: {str(e)}")
                continue
        
        return items
    
    def _get_xml_text(self, element: ET.Element, possible_tags: List[str]) -> Optional[str]:
        """
        Get text content from XML element by trying multiple possible tag names
        
        Args:
            element: XML element to search in
            possible_tags: List of possible tag names to try
            
        Returns:
            Text content or None if not found
        """
        for tag in possible_tags:
            found = element.find(tag)
            if found is not None and found.text:
                return found.text.strip()
        return None
    
    def _parse_price(self, price_str: str) -> float:
        """
        Parse price string to float
        
        Args:
            price_str: Price as string
            
        Returns:
            Price as float
        """
        try:
            if not price_str:
                return 0.0
            # Remove currency symbols and extra whitespace
            cleaned = price_str.replace('₪', '').replace('$', '').replace(',', '').strip()
            return float(cleaned)
        except (ValueError, TypeError):
            return 0.0