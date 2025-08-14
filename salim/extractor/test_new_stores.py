#!/usr/bin/env python3
"""
Test script to verify the extractor works with files from the new stores
"""

import sys
import os
import json
import gzip
from pathlib import Path

# Add the extractor directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.xml_processor import XMLProcessor
from src.normalizer import DataNormalizer

def test_store(file_path: str, provider: str, file_type: str):
    """
    Test processing a file from a specific store
    
    Args:
        file_path: Path to .gz file
        provider: Store name (wolt, superpharm, shufersal)
        file_type: Either 'pricesFull' or 'promoFull'
    """
    print(f"\n{'='*60}")
    print(f"Testing {provider.upper()} - {file_type}")
    print(f"File: {file_path}")
    print("="*60)
    
    try:
        # Check if file exists
        if not Path(file_path).exists():
            print(f"❌ File not found: {file_path}")
            return False
            
        # Read and decompress the file
        with gzip.open(file_path, 'rb') as f:
            xml_content = f.read()
        print(f"✅ File read successfully ({len(xml_content)} bytes)")
        
        # Initialize processors
        xml_processor = XMLProcessor()
        normalizer = DataNormalizer()
        
        # Parse XML
        parsed_data = xml_processor.parse(xml_content, provider, file_type)
        items_count = len(parsed_data.get('items', []))
        print(f"✅ XML parsed: {items_count} items found")
        
        # Show sample item
        if parsed_data['items']:
            print("\n📄 Sample item (first):")
            first_item = parsed_data['items'][0]
            for key, value in list(first_item.items())[:5]:
                print(f"  {key}: {value}")
        
        # Normalize data
        normalized = normalizer.normalize(
            parsed_data,
            provider,
            'test-branch',
            file_type
        )
        print(f"\n✅ Data normalized: {len(normalized)} items")
        
        # Show normalized sample
        if normalized:
            print("\n📊 Normalized sample (first):")
            first_normalized = normalized[0]
            for key, value in list(first_normalized.items())[:5]:
                print(f"  {key}: {value}")
        
        # Save test output
        output_dir = Path('test_output')
        output_dir.mkdir(exist_ok=True)
        output_file = output_dir / f"{provider}_{file_type}_test.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'provider': provider,
                'type': file_type,
                'items_count': len(normalized),
                'sample_items': normalized[:3]
            }, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Test output saved to: {output_file}")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_new_stores():
    """Test all new stores with sample files"""
    
    print("\n" + "="*70)
    print("🧪 TESTING EXTRACTOR WITH NEW STORES")
    print("="*70)
    
    # Define test files (you'll need to download these first)
    test_files = [
        # Wolt files
        {
            'path': '/home/amirkhalifa/data-pipeline-2025/salim/crawler/downloads/Stores_2025-08-15.gz',
            'provider': 'wolt',
            'type': 'stores'
        },
        {
            'path': '/home/amirkhalifa/data-pipeline-2025/salim/crawler/downloads/PriceFull_2025-08-15_1.gz',
            'provider': 'wolt',
            'type': 'pricesFull'
        },
        {
            'path': '/home/amirkhalifa/data-pipeline-2025/salim/crawler/downloads/PromoFull_2025-08-15_1.gz',
            'provider': 'wolt',
            'type': 'promoFull'
        },
        # Add SuperPharm and Shufersal files when available
    ]
    
    results = {}
    
    for test_file in test_files:
        if Path(test_file['path']).exists():
            success = test_store(
                test_file['path'],
                test_file['provider'],
                test_file['type']
            )
            results[f"{test_file['provider']}_{test_file['type']}"] = success
        else:
            print(f"\n⚠️ Skipping {test_file['path']} (file not found)")
    
    # Summary
    print("\n" + "="*70)
    print("📊 TEST SUMMARY")
    print("="*70)
    
    for test_name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{test_name}: {status}")
    
    # Overall result
    all_passed = all(results.values()) if results else False
    
    print("\n" + "="*70)
    if all_passed:
        print("✅ ALL TESTS PASSED - Extractor ready for new stores!")
    elif results:
        print("⚠️ SOME TESTS FAILED - Review the errors above")
    else:
        print("⚠️ NO TESTS RUN - Download test files first")
    print("="*70)

def main():
    """Main execution"""
    if len(sys.argv) > 1:
        # Test specific file
        if len(sys.argv) != 4:
            print("Usage: python test_new_stores.py <file_path> <provider> <file_type>")
            print("Example: python test_new_stores.py file.gz wolt pricesFull")
            sys.exit(1)
        
        file_path = sys.argv[1]
        provider = sys.argv[2]
        file_type = sys.argv[3]
        
        success = test_store(file_path, provider, file_type)
        sys.exit(0 if success else 1)
    else:
        # Test all new stores
        test_new_stores()

if __name__ == "__main__":
    main()
