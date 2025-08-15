import json
import logging
from typing import Dict, List, Any
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

class DataNormalizer:
    """Normalizes extracted data to required JSON format"""
    
    def __init__(self):
        self.provider_mapping = {
            '7290055700007': 'carrefour',
            '7290058197699': 'goodpharm', 
            '7290103152017': 'osherad',
            '7290873255550': 'tivtaam',
            '7290803800003': 'yohananof',
            '7290058173198': 'zolbegadol'
        }
        
        self.branch_mapping = {
            '0004': 'תל אביב - יפו',
            '1112': 'ירושלים',
            '2960': 'חיפה',
            '3440': 'באר שבע',
            '400': 'אשדוד',
            '970': 'פתח תקווה',
            '010': 'רמת גן',
            '022': 'חולון',
            '023': 'ראשון לציון',
            '002': 'נתניה',
            '007': 'הרצליה',
            '008': 'רעננה',
            '051': 'פתח תקווה',
            '005': 'תל אביב - יפו',
            '095': 'חיפה',
            '042': 'באר שבע'
        }
    
    def get_provider_name(self, chain_id: str) -> str:
        """Get provider name from chain ID"""
        return self.provider_mapping.get(chain_id, chain_id)
    
    def get_branch_name(self, store_id: str) -> str:
        """Get branch name from store ID"""
        return self.branch_mapping.get(store_id, store_id)
    
    def parse_timestamp(self, filename: str) -> str:
        """Extract timestamp from filename and convert to ISO format"""
        try:
            # Handle different filename formats:
            # 1. Old format: Price7290055700007-0004-202508071000.gz
            # 2. New format: pricesFull_2025-08-06T18:00:00Z.gz
            
            # First, try to find ISO timestamp in the filename
            import re
            iso_match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)', filename)
            if iso_match:
                return iso_match.group(1)
            
            # Try to find the new format: pricesFull_2025-08-06-18-00-00.gz
            new_format_match = re.search(r'(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})', filename)
            if new_format_match:
                year, month, day, hour, minute, second = new_format_match.groups()
                timestamp = f"{year}-{month}-{day}T{hour}:{minute}:{second}Z"
                return timestamp
            
            # If no ISO timestamp, try to parse the old format
            parts = filename.split('-')
            if len(parts) >= 3:
                # Look for the date part (should be 8 digits: YYYYMMDD)
                for part in parts:
                    if len(part) == 8 and part.isdigit() and part.startswith('20'):  # Year should start with 20
                        date_part = part
                        break
                else:
                    # If no 8-digit date found, try to find it in the filename
                    # Look specifically for a date pattern that starts with 20 (year 2000+)
                    date_match = re.search(r'(20\d{6})', filename)
                    if date_match:
                        date_part = date_match.group(1)
                    else:
                        raise ValueError("No date pattern found in filename")
                
                # Find time part (should be 4 digits: HHMM)
                # Look for time pattern after the date part
                time_match = re.search(rf'{date_part}-(\d{{4}})', filename)
                if time_match:
                    time_part = time_match.group(1)
                else:
                    # If no time found after date, look for any 4-digit time pattern
                    # But exclude the date part and chain ID parts
                    time_match = re.search(r'(\d{4})', filename)
                    if time_match:
                        potential_time = time_match.group(1)
                        # Validate that this looks like a time (HHMM where HH < 24 and MM < 60)
                        if (len(potential_time) == 4 and 
                            potential_time.isdigit() and
                            int(potential_time[:2]) < 24 and 
                            int(potential_time[2:4]) < 60):
                            time_part = potential_time
                        else:
                            time_part = "0000"  # Default to midnight
                    else:
                        time_part = "0000"  # Default to midnight
                
                logger.debug(f"Parsed date_part: {date_part}, time_part: {time_part}")
                
                # Parse date: YYYYMMDD
                year = date_part[:4]
                month = date_part[4:6]
                day = date_part[6:8]
                
                # Parse time: HHMM
                hour = time_part[:2]
                minute = time_part[2:4]
                
                # Create ISO timestamp
                timestamp = f"{year}-{month}-{day}T{hour}:{minute}:00Z"
                logger.debug(f"Generated timestamp: {timestamp}")
                return timestamp
        except Exception as e:
            logger.warning(f"Failed to parse timestamp from filename {filename}: {e}")
        
        # Fallback to current time
        return datetime.utcnow().isoformat() + "Z"
    
    def detect_file_type_from_filename(self, filename: str) -> str:
        """Detect file type from filename"""
        filename_lower = filename.lower()
        if 'pricesfull' in filename_lower or 'price' in filename_lower:
            return 'pricesFull'
        elif 'promofull' in filename_lower or 'promo' in filename_lower:
            return 'promoFull'
        else:
            return 'unknown'
    
    def normalize_items(self, items: List[Dict], metadata: Dict) -> List[Dict]:
        """Normalize items to required format"""
        normalized_items = []
        
        for item in items:
            normalized_item = {
                'product': item.get('product', ''),
                'price': item.get('price', 0.0),
                'unit': item.get('unit', ''),
                'quantity': item.get('quantity', ''),
                'item_code': item.get('item_code', ''),
                'manufacturer': item.get('manufacturer', ''),
                'update_date': item.get('update_date', '')
            }
            normalized_items.append(normalized_item)
        
        return normalized_items
    
    def normalize_promotions(self, promotions: List[Dict], metadata: Dict) -> List[Dict]:
        """Normalize promotions to required format"""
        normalized_promotions = []
        
        for promo in promotions:
            normalized_promo = {
                'promotion_id': promo.get('promotion_id', ''),
                'description': promo.get('description', ''),
                'start_date': promo.get('start_date', ''),
                'end_date': promo.get('end_date', ''),
                'discount_type': promo.get('discount_type', ''),
                'discount_value': promo.get('discount_value', '')
            }
            normalized_promotions.append(normalized_promo)
        
        return normalized_promotions
    
    def create_output_json(self, metadata: Dict, content: List[Dict], filename: str) -> Dict[str, Any]:
        """Create the final output JSON in required format"""
        
        # Extract provider and branch info
        chain_id = metadata.get('ChainId', '')
        store_id = metadata.get('StoreId', '')
        file_type = metadata.get('type', '')
        
        provider = self.get_provider_name(chain_id)
        branch = self.get_branch_name(store_id)
        timestamp = self.parse_timestamp(filename)
        
        # Create base structure
        output = {
            'provider': provider,
            'branch': branch,
            'type': file_type,
            'timestamp': timestamp,
            'metadata': {
                'chain_id': chain_id,
                'store_id': store_id,
                'xml_version': metadata.get('XmlDocVersion', ''),
                'bikoret_no': metadata.get('BikoretNo', ''),
                'dll_version': metadata.get('DllVerNo', '')
            }
        }
        
        # Add content based on type
        if file_type == 'pricesFull':
            output['items'] = self.normalize_items(content, metadata)
            output['items_count'] = metadata.get('items_count', 0)
        elif file_type == 'promoFull':
            output['promotions'] = self.normalize_promotions(content, metadata)
            output['promotions_count'] = metadata.get('promotions_count', 0)
        
        return output
    
    def save_json_locally(self, data: Dict, output_path: str):
        """Save JSON data to local file for review"""
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved JSON to: {output_path}")
        except Exception as e:
            logger.error(f"Failed to save JSON to {output_path}: {e}")
            raise
