import logging
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class DataNormalizer:
    """Normalize extracted data into standardized format"""
    
    def __init__(self):
        pass
    
    def normalize_data(self, raw_data: Dict[str, Any], file_metadata: Dict[str, str]) -> Dict[str, Any]:
        """
        Normalize raw extracted data into the required JSON format
        
        Args:
            raw_data: Raw extracted data from file processor
            file_metadata: File metadata
            
        Returns:
            Normalized data dictionary in required format
        """
        try:
            logger.info(f"Normalizing data for file: {file_metadata.get('filename', 'unknown')}")
            
            # Convert timestamp to ISO format
            iso_timestamp = self._convert_timestamp(file_metadata.get('timestamp', ''))
            
            # Normalize items
            normalized_items = self._normalize_items(raw_data.get('items', []))
            
            # Create the required JSON structure
            normalized_data = {
                'provider': raw_data.get('provider', ''),
                'branch': raw_data.get('branch', ''),
                'type': raw_data.get('type', ''),
                'timestamp': iso_timestamp,
                'items': normalized_items,
                'metadata': {
                    'total_items': len(normalized_items),
                    'processed_at': datetime.utcnow().isoformat() + 'Z',
                    'source_file': file_metadata.get('filename', ''),
                    'file_size': file_metadata.get('size', 0)
                }
            }
            
            logger.info(f"Successfully normalized {len(normalized_items)} items")
            return normalized_data
            
        except Exception as e:
            logger.error(f"Error normalizing data: {str(e)}")
            return None
    
    def _convert_timestamp(self, timestamp_str: str) -> str:
        """
        Convert timestamp string to ISO format
        
        Args:
            timestamp_str: Timestamp string (e.g., "202509020930")
            
        Returns:
            ISO formatted timestamp
        """
        try:
            if not timestamp_str or len(timestamp_str) != 12:
                return datetime.utcnow().isoformat() + 'Z'
            
            # Parse format: YYYYMMDDHHMM
            year = int(timestamp_str[:4])
            month = int(timestamp_str[4:6])
            day = int(timestamp_str[6:8])
            hour = int(timestamp_str[8:10])
            minute = int(timestamp_str[10:12])
            
            dt = datetime(year, month, day, hour, minute)
            return dt.isoformat() + 'Z'
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error parsing timestamp '{timestamp_str}': {str(e)}, using current time")
            return datetime.utcnow().isoformat() + 'Z'
    
    def _normalize_items(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalize item list to consistent format
        
        Args:
            items: List of raw item dictionaries
            
        Returns:
            List of normalized item dictionaries
        """
        normalized_items = []
        
        for item in items:
            try:
                normalized_item = {
                    'product': self._clean_product_name(item.get('product', '')),
                    'price': float(item.get('price', 0.0)),
                    'unit': self._normalize_unit(item.get('unit', 'unit'))
                }
                
                # Add optional fields if they exist
                if item.get('original_price') is not None:
                    normalized_item['original_price'] = float(item.get('original_price', 0.0))
                
                if item.get('discount'):
                    normalized_item['discount'] = str(item.get('discount', ''))
                
                # Only add valid items (must have product name and price > 0)
                if normalized_item['product'] and normalized_item['price'] > 0:
                    normalized_items.append(normalized_item)
                    
            except Exception as e:
                logger.warning(f"Error normalizing item {item}: {str(e)}")
                continue
        
        return normalized_items
    
    def _clean_product_name(self, product_name: str) -> str:
        """
        Clean and standardize product name
        
        Args:
            product_name: Raw product name
            
        Returns:
            Cleaned product name
        """
        if not product_name:
            return ""
        
        # Remove extra whitespace and newlines
        cleaned = " ".join(product_name.strip().split())
        
        # Remove common prefixes/suffixes that add noise
        # You can expand this based on your data patterns
        return cleaned
    
    def _normalize_unit(self, unit: str) -> str:
        """
        Normalize unit of measurement
        
        Args:
            unit: Raw unit string
            
        Returns:
            Normalized unit
        """
        if not unit:
            return "unit"
        
        unit_lower = unit.lower().strip()
        
        # Common unit normalizations
        unit_mappings = {
            'kg': 'kilogram',
            'gr': 'gram',
            'g': 'gram',
            'liter': 'liter',
            'l': 'liter',
            'ml': 'milliliter',
            'יחידה': 'unit',
            'יח': 'unit',
            'חבילה': 'package',
            'בקבוק': 'bottle'
        }
        
        return unit_mappings.get(unit_lower, unit_lower)