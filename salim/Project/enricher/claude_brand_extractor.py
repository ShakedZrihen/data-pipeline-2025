import os
import json
import logging
from typing import Dict, Optional
from dotenv import load_dotenv
import anthropic

# Load environment variables
load_dotenv('../.env')

logger = logging.getLogger(__name__)

class ClaudeBrandExtractor:
    """Brand extractor using Claude API"""
    
    def __init__(self):
        self.api_key = os.getenv('CLAUDE_API_KEY')
        if not self.api_key:
            raise ValueError("CLAUDE_API_KEY not found in environment variables")
        
        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.model = "claude-3-5-sonnet-20241022"  # Using the latest Claude model
        
    def extract_brand(self, item_name: str, description: str = "") -> Dict[str, any]:
        """Extract brand from item name and description using Claude API"""
        
        # Prepare the prompt
        prompt = f"""You are a brand extraction expert. Given an item name and description, extract the brand name.

Item Name: {item_name}
Description: {description}

Instructions:
1. Identify the brand/manufacturer name from the item name and description
2. Return only the brand name, nothing else
3. If no clear brand is found, return "Unknown"
4. For Hebrew brands, return the Hebrew name
5. For international brands, return the standard brand name

Examples:
- "קשקבל במשקל משק יעקבס" → "יעקבס"
- "קוקה קולה בקבוק 500" → "קוקה קולה"
- "ביסלי גריל 200גרם" → "אסם"
- "חלב תנובה 3%" → "תנובה"

Brand:"""

        try:
            # Make API call to Claude
            response = self.client.messages.create(
                model=self.model,
                max_tokens=50,
                temperature=0.1,  # Low temperature for consistent results
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            )
            
            # Extract the brand from response
            brand = response.content[0].text.strip()
            
            # Clean up the response
            if brand.lower() in ['unknown', 'none', 'n/a', '']:
                brand = 'Unknown'
            
            return {
                'brand': brand,
                'confidence': 0.9,  # High confidence for AI extraction
                'source_field': 'claude_api',
                'extraction_method': 'claude_3_5_sonnet'
            }
            
        except Exception as e:
            logger.warning(f"Claude API extraction failed: {e}")
            # Try fallback extraction methods
            return self._fallback_brand_extraction(item_name, description)
    
    def _fallback_brand_extraction(self, item_name: str, description: str = "") -> Dict[str, any]:
        """Fallback brand extraction using simple heuristics when Claude API fails"""
        try:
            # Combine item name and description for analysis
            text = f"{item_name} {description}".lower()
            
            # Known brand patterns (Hebrew and English)
            brand_patterns = {
                'תנובה': ['תנובה', 'tnuva'],
                'שטראוס': ['שטראוס', 'strauss'],
                'אסם': ['אסם', 'osem', 'ביסלי', 'bissli'],
                'קוקה קולה': ['קוקה קולה', 'coca cola', 'coke'],
                'פפסי': ['פפסי', 'pepsi'],
                'נסטלה': ['נסטלה', 'nestle'],
                'יוניליוור': ['יוניליוור', 'unilever'],
                'פרוקטר אנד גמבל': ['פרוקטר', 'procter', 'gamble', 'p&g'],
                'דן': ['דן', 'dan'],
                'יעקבס': ['יעקבס', 'jacobs'],
                'קפה טורקי': ['קפה טורקי', 'turkish coffee'],
                'מגדל': ['מגדל', 'migdal'],
                'טבע': ['טבע', 'teva'],
                'סופר פארם': ['סופר פארם', 'super pharm'],
                'רמי לוי': ['רמי לוי', 'rami levi'],
                'שופרסל': ['שופרסל', 'shufersal'],
                'ויקטורי': ['ויקטורי', 'victory'],
                'דור אלון': ['דור אלון', 'dor alon'],
                'מגה': ['מגה', 'mega'],
                'אושר עד': ['אושר עד', 'osher ad']
            }
            
            # Try to find brand matches
            for brand, patterns in brand_patterns.items():
                for pattern in patterns:
                    if pattern in text:
                        return {
                            'brand': brand,
                            'confidence': 0.6,  # Medium confidence for pattern matching
                            'source_field': 'fallback_pattern',
                            'extraction_method': 'pattern_matching'
                        }
            
            # If no pattern matches, try to extract from manufacturer field
            if description:
                # Look for common manufacturer indicators
                manufacturer_indicators = ['יצרן:', 'manufacturer:', 'by:', 'על ידי:']
                for indicator in manufacturer_indicators:
                    if indicator in description.lower():
                        # Extract text after the indicator
                        parts = description.lower().split(indicator)
                        if len(parts) > 1:
                            potential_brand = parts[1].strip().split()[0]  # First word after indicator
                            if len(potential_brand) > 2:  # Reasonable brand name length
                                return {
                                    'brand': potential_brand.title(),
                                    'confidence': 0.4,  # Lower confidence for manufacturer extraction
                                    'source_field': 'manufacturer_field',
                                    'extraction_method': 'manufacturer_extraction'
                                }
            
            # Last resort: try to extract first meaningful word from item name
            words = item_name.split()
            for word in words:
                if len(word) > 3 and not word.isdigit():  # Skip short words and numbers
                    return {
                        'brand': word,
                        'confidence': 0.2,  # Very low confidence
                        'source_field': 'first_word',
                        'extraction_method': 'first_word_extraction'
                    }
            
            # If all else fails
            return {
                'brand': 'Unknown',
                'confidence': 0.0,
                'source_field': 'fallback_failed',
                'extraction_method': 'no_extraction'
            }
            
        except Exception as e:
            logger.error(f"Fallback brand extraction failed: {e}")
            return {
                'brand': 'Unknown',
                'confidence': 0.0,
                'source_field': 'fallback_error',
                'extraction_method': 'fallback_error'
            }
    
    def enrich_item_with_brand(self, item_data: Dict[str, any]) -> Dict[str, any]:
        """Enrich item with Claude API-extracted brand"""
        item_name = item_data.get('item_name', '') or item_data.get('ItemName', '')
        description = item_data.get('item_description', '') or item_data.get('ManufacturerItemDescription', '')
        
        brand_info = self.extract_brand(item_name, description)
        
        # Update item data
        item_data['item_brand'] = brand_info['brand']
        item_data['brand_confidence'] = brand_info['confidence']
        item_data['brand_source'] = brand_info['source_field']
        item_data['brand_extraction_method'] = brand_info['extraction_method']
        
        return item_data

def main():
    """Test the Claude brand extractor"""
    logging.basicConfig(level=logging.INFO)
    
    try:
        extractor = ClaudeBrandExtractor()
        
        # Test cases
        test_cases = [
            ("קשקבל במשקל משק יעקבס", ""),
            ("קוקה קולה בקבוק 500", "משקה קולה"),
            ("ביסלי גריל 200גרם", "חטיף מלוח"),
            ("חלב תנובה 3%", "חלב טרי"),
            ("שניצל דניאל 400גרם", "שניצל עוף")
        ]
        
        print("Testing Claude Brand Extractor:")
        for item_name, description in test_cases:
            result = extractor.extract_brand(item_name, description)
            print(f"  • {item_name}")
            print(f"    → Brand: {result['brand']}")
            print(f"    → Confidence: {result['confidence']}")
            print(f"    → Method: {result['extraction_method']}")
            print()
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
