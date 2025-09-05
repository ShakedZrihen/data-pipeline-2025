import json
import logging
import time
from typing import Dict, List, Any, Optional
import openai
from dataclasses import dataclass

# Configure logging
logger = logging.getLogger(__name__)

@dataclass
class EnrichmentResult:
    """Result of product enrichment"""
    manufacturer: Optional[str] = None
    category: Optional[str] = None
    is_kosher: Optional[bool] = None


class OpenAIEnricher:
    """OpenAI-based product enricher for Israeli supermarket data"""
    
    def __init__(self, api_key: str, model: str = "gpt-3.5-turbo"):
        """
        Initialize OpenAI enricher
        
        Args:
            api_key: OpenAI API key
            model: OpenAI model to use (default: gpt-3.5-turbo)
        """
        self.client = openai.OpenAI(api_key=api_key)
        self.model = model
        self.batch_size = 10  # Reduced from 50 to prevent timeouts
        self.max_retries = 3
        self.retry_delay = 1  # seconds
        
    def enrich_items(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich a list of product items with OpenAI
        
        Args:
            items: List of product dictionaries
            
        Returns:
            List of enriched product dictionaries
        """
        if not items:
            return items
            
        logger.info(f"Starting enrichment for {len(items)} items")
        enriched_items = []
        
        # Process in batches of 50
        for i in range(0, len(items), self.batch_size):
            batch = items[i:i + self.batch_size]
            logger.info(f"Processing batch {i//self.batch_size + 1}: items {i+1}-{min(i+len(batch), len(items))}")
            
            try:
                enriched_batch = self._process_batch(batch)
                enriched_items.extend(enriched_batch)
                
            except Exception as e:
                logger.error(f"Batch processing failed: {e}")
                # Fallback: process items individually
                logger.info("Falling back to individual processing")
                for item in batch:
                    try:
                        enriched_item = self._enrich_single_item(item)
                        enriched_items.append(enriched_item)
                    except Exception as single_error:
                        logger.warning(f"Individual enrichment failed for '{item.get('product_name', 'unknown')}': {single_error}")
                        # Return original item without enrichment
                        enriched_items.append(item)
            
            # Rate limiting delay between batches
            if i + self.batch_size < len(items):
                time.sleep(0.5)
                
        logger.info(f"Enrichment completed for {len(enriched_items)} items")
        return enriched_items
    
    def _process_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process a batch of items with OpenAI"""
        
        # Prepare products for API call
        products_for_api = []
        for idx, item in enumerate(batch):
            product_name = item.get('product_name', '').strip()
            if product_name:
                products_for_api.append({
                    "id": idx,
                    "product_name": product_name
                })
        
        if not products_for_api:
            logger.warning("No valid product names found in batch")
            return batch
        
        # Create prompt
        prompt = self._create_batch_prompt(products_for_api)
        
        # Call OpenAI with retries
        enrichments = self._call_openai_with_retry(prompt)
        
        # Merge enrichments back into original items
        enriched_batch = []
        for idx, item in enumerate(batch):
            enriched_item = item.copy()
            
            # Find matching enrichment by id
            matching_enrichment = next(
                (e for e in enrichments if e.get('id') == idx), 
                None
            )
            
            if matching_enrichment:
                enriched_item['manufacturer'] = matching_enrichment.get('manufacturer') or enriched_item.get('manufacturer')
                enriched_item['category'] = matching_enrichment.get('category') or enriched_item.get('category')
                enriched_item['is_kosher'] = matching_enrichment.get('is_kosher') if matching_enrichment.get('is_kosher') is not None else enriched_item.get('is_kosher')
            
            enriched_batch.append(enriched_item)
            
        return enriched_batch
    
    def _create_batch_prompt(self, products: List[Dict]) -> str:
        """Create prompt for batch processing"""
        
        products_json = json.dumps(products, ensure_ascii=False, indent=2)
        
        prompt = f"""
Analyze these {len(products)} Israeli supermarket products and return enrichment data.

For each product, determine:
- manufacturer: The brand/company name (string, or null if unknown)
- category: Food/product category like "Beverages", "Dairy", "Snacks", "Meat", "Vegetables", etc. (string, or null if unknown)  
- is_kosher: Whether the product is kosher certified (true/false, or null if uncertain)

Consider Hebrew product names and Israeli brands like Osem, Tnuva, Elite, Strauss, etc.

Products to analyze:
{products_json}

Return ONLY a valid JSON array with this exact structure:
[
    {{"id": 0, "manufacturer": "Coca-Cola", "category": "Beverages", "is_kosher": true}},
    {{"id": 1, "manufacturer": "Osem", "category": "Snacks", "is_kosher": true}},
    ...
]

Important: Include ALL {len(products)} products in your response, maintaining the same id order.
"""
        return prompt
    
    def _call_openai_with_retry(self, prompt: str) -> List[Dict]:
        """Call OpenAI API with retry logic"""
        
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"OpenAI API call attempt {attempt + 1}")
                
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are an expert on Israeli supermarket products and food classification."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,  # Low temperature for consistent results
                    max_tokens=4000
                )
                
                content = response.choices[0].message.content.strip()
                
                # Parse JSON response
                try:
                    # Clean markdown code blocks from response
                    cleaned_content = content.strip()
                    if cleaned_content.startswith("```json"):
                        cleaned_content = cleaned_content[7:]  # Remove ```json
                    if cleaned_content.startswith("```"):
                        cleaned_content = cleaned_content[3:]  # Remove ```
                    if cleaned_content.endswith("```"):
                        cleaned_content = cleaned_content[:-3]  # Remove ```
                    cleaned_content = cleaned_content.strip()
                    
                    enrichments = json.loads(cleaned_content)
                    if isinstance(enrichments, list):
                        logger.debug(f"Successfully parsed {len(enrichments)} enrichments")
                        return enrichments
                    else:
                        raise ValueError("Response is not a JSON array")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse OpenAI response as JSON: {e}")
                    logger.error(f"Response content: {content[:500]}...")
                    raise ValueError(f"Invalid JSON response: {e}")
                    
            except openai.RateLimitError as e:
                logger.warning(f"Rate limit hit, waiting {self.retry_delay * (attempt + 1)} seconds")
                time.sleep(self.retry_delay * (attempt + 1))
                continue
                
            except openai.APIError as e:
                logger.error(f"OpenAI API error: {e}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.retry_delay * (attempt + 1))
                continue
                
            except Exception as e:
                logger.error(f"Unexpected error calling OpenAI: {e}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.retry_delay * (attempt + 1))
                continue
        
        raise Exception(f"Failed to get response from OpenAI after {self.max_retries} attempts")
    
    def _enrich_single_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Fallback: enrich a single item"""
        product_name = item.get('product_name', '').strip()
        
        if not product_name:
            logger.warning("Empty product name, skipping enrichment")
            return item
        
        prompt = f"""
Analyze this Israeli supermarket product: "{product_name}"

Determine:
- manufacturer: Brand/company name (or null if unknown)
- category: Food category (or null if unknown)
- is_kosher: true/false/null if uncertain

Return ONLY valid JSON:
{{"manufacturer": "...", "category": "...", "is_kosher": true}}
"""
        
        try:
            enrichments = self._call_openai_with_retry(prompt)
            
            # For single item, response should be a dict, not array
            if isinstance(enrichments, list) and len(enrichments) > 0:
                enrichment = enrichments[0]
            elif isinstance(enrichments, dict):
                enrichment = enrichments
            else:
                logger.warning("Unexpected response format for single item")
                return item
            
            enriched_item = item.copy()
            enriched_item['manufacturer'] = enrichment.get('manufacturer') or item.get('manufacturer')
            enriched_item['category'] = enrichment.get('category') or item.get('category')
            enriched_item['is_kosher'] = enrichment.get('is_kosher') if enrichment.get('is_kosher') is not None else item.get('is_kosher')
            
            return enriched_item
            
        except Exception as e:
            logger.error(f"Single item enrichment failed for '{product_name}': {e}")
            return item