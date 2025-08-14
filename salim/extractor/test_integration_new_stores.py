#!/usr/bin/env python3
"""
Integration test to verify extractor handles new stores correctly
Creates sample XML data to test parsers
"""

import sys
import os
import gzip
import tempfile
from pathlib import Path

# Add the extractor directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.xml_processor import XMLProcessor
from src.normalizer import DataNormalizer

def create_sample_wolt_xml():
    """Create sample Wolt XML for testing"""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<Prices>
    <ChainId>7290058249350</ChainId>
    <StoreId>001</StoreId>
    <Products>
        <Product>
            <ItemCode>1234567890123</ItemCode>
            <ItemName>Sample Product Wolt</ItemName>
            <ItemPrice>10.50</ItemPrice>
            <ManufacturerName>Test Manufacturer</ManufacturerName>
            <Quantity>1</Quantity>
            <UnitMeasure>unit</UnitMeasure>
        </Product>
    </Products>
</Prices>"""

def create_sample_superpharm_xml():
    """Create sample SuperPharm XML for testing"""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<Root>
    <ChainId>7290172900007</ChainId>
    <StoreId>013</StoreId>
    <Products>
        <Product>
            <ItemCode>9876543210987</ItemCode>
            <ItemName>Sample Product SuperPharm</ItemName>
            <ItemPrice>25.90</ItemPrice>
            <ManufacturerName>Pharm Corp</ManufacturerName>
            <Quantity>1</Quantity>
        </Product>
    </Products>
</Root>"""

def create_sample_shufersal_xml():
    """Create sample Shufersal XML for testing"""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<Prices>
    <ChainId>7290027600007</ChainId>
    <StoreId>001</StoreId>
    <Items>
        <Item>
            <ItemId>5555555555555</ItemId>
            <ItemName>Sample Product Shufersal</ItemName>
            <ItemPrice>15.00</ItemPrice>
            <ManufacturerName>Shufersal Brand</ManufacturerName>
            <QtyInPackage>1</QtyInPackage>
            <UnitOfMeasure>unit</UnitOfMeasure>
        </Item>
    </Items>
</Prices>"""

def test_store_parser(store_name, xml_content, file_type='pricesFull'):
    """Test a specific store's parser"""
    
    print(f"\n{'='*60}")
    print(f"Testing {store_name.upper()} Parser")
    print("="*60)
    
    try:
        # Initialize processors
        xml_processor = XMLProcessor()
        normalizer = DataNormalizer()
        
        # Parse XML
        parsed_data = xml_processor.parse(xml_content, store_name, file_type)
        items = parsed_data.get('items', [])
        metadata = parsed_data.get('metadata', {})
        
        print(f"✅ XML parsed successfully")
        print(f"   - Items found: {len(items)}")
        print(f"   - Metadata: {metadata}")
        
        # Show parsed item
        if items:
            print(f"\n📄 Parsed item:")
            for key, value in items[0].items():
                print(f"   {key}: {value}")
        
        # Normalize data
        normalized = normalizer.normalize(
            parsed_data,
            store_name,
            'test-branch',
            file_type
        )
        
        print(f"\n✅ Data normalized successfully")
        print(f"   - Normalized items: {len(normalized)}")
        
        # Show normalized item
        if normalized:
            print(f"\n📊 Normalized item:")
            for key, value in list(normalized[0].items())[:8]:
                print(f"   {key}: {value}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run integration tests for all new stores"""
    
    print("\n" + "="*70)
    print("🧪 EXTRACTOR INTEGRATION TEST - NEW STORES")
    print("="*70)
    
    # Test data for each store
    test_cases = [
        {
            'name': 'wolt',
            'xml': create_sample_wolt_xml(),
            'type': 'pricesFull'
        },
        {
            'name': 'superpharm',
            'xml': create_sample_superpharm_xml(),
            'type': 'pricesFull'
        },
        {
            'name': 'shufersal',
            'xml': create_sample_shufersal_xml(),
            'type': 'pricesFull'
        }
    ]
    
    results = {}
    
    # Test each store
    for test_case in test_cases:
        success = test_store_parser(
            test_case['name'],
            test_case['xml'],
            test_case['type']
        )
        results[test_case['name']] = success
    
    # Summary
    print("\n" + "="*70)
    print("📊 INTEGRATION TEST SUMMARY")
    print("="*70)
    
    for store, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{store.upper()}: {status}")
    
    all_passed = all(results.values())
    
    print("\n" + "="*70)
    if all_passed:
        print("✅ ALL TESTS PASSED!")
        print("\nThe extractor is ready to handle files from:")
        print("  • Wolt")
        print("  • SuperPharm")
        print("  • Shufersal")
        print("\nNo additional changes needed in the extractor!")
    else:
        print("⚠️ SOME TESTS FAILED")
        print("Review the errors above and fix the parsers")
    print("="*70)
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
