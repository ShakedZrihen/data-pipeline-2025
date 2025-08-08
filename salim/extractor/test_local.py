#!/usr/bin/env python3
"""
Local Testing Script for Extractor Lambda
Tests the entire pipeline without AWS services
"""

import os
import sys
import json
import gzip
import shutil
from pathlib import Path
from datetime import datetime, timezone

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

# Import our modules
from src.xml_processor import XMLProcessor
from src.normalizer import DataNormalizer


def test_with_local_file(gz_file_path: str, provider: str = "victory", 
                         branch: str = "tel-aviv", file_type: str = "pricesFull"):
    """
    Test the extractor with a local .gz file
    
    Args:
        gz_file_path: Path to the .gz file
        provider: Provider name
        branch: Branch name
        file_type: File type (pricesFull or promoFull)
    """
    print("=" * 70)
    print("LOCAL EXTRACTOR TEST")
    print("=" * 70)
    print(f"File: {gz_file_path}")
    print(f"Provider: {provider}")
    print(f"Branch: {branch}")
    print(f"Type: {file_type}")
    print("-" * 70)
    
    # Check file exists
    if not Path(gz_file_path).exists():
        print(f"ERROR: File not found: {gz_file_path}")
        return
    
    try:
        # Step 1: Read and decompress the file
        print("\n1. Reading and decompressing file...")
        with gzip.open(gz_file_path, 'rb') as f:
            xml_content = f.read()
        print(f"   ✓ Decompressed {len(xml_content)} bytes")
        
        # Step 2: Parse XML
        print("\n2. Parsing XML...")
        xml_processor = XMLProcessor()
        parsed_data = xml_processor.parse(xml_content, provider, file_type)
        items_count = len(parsed_data.get('items', []))
        print(f"   ✓ Parsed {items_count} items")
        
        # Step 3: Normalize data
        print("\n3. Normalizing data...")
        normalizer = DataNormalizer()
        normalized_items = normalizer.normalize(parsed_data, provider, branch, file_type)
        print(f"   ✓ Normalized {len(normalized_items)} items")
        
        # Step 4: Create JSON message (what would be sent to SQS)
        print("\n4. Creating JSON message...")
        message = {
            "provider": provider,
            "branch": branch,
            "type": file_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "items": normalized_items,
            "source_file": f"providers/{provider}/{branch}/{Path(gz_file_path).name}",
            "items_count": len(normalized_items)
        }
        
        # Step 5: Save JSON locally
        output_dir = Path("test_output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"{provider}_{branch}_{file_type}_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(message, f, ensure_ascii=False, indent=2)
        
        print(f"   ✓ JSON saved to: {output_file}")
        
        # Step 6: Print sample output
        print("\n5. Sample output (first 3 items):")
        print("-" * 70)
        sample_message = {
            **message,
            "items": normalized_items[:3]  # Only show first 3 items
        }
        print(json.dumps(sample_message, indent=2, ensure_ascii=False))
        
        print("\n" + "=" * 70)
        print("✅ TEST SUCCESSFUL!")
        print(f"Total items processed: {len(normalized_items)}")
        print(f"Output saved to: {output_file}")
        print("=" * 70)
        
        return output_file
        
    except gzip.BadGzipFile:
        print("\n❌ ERROR: File is not a valid gzip file")
        print("This might be an HTML error page saved as .gz")
        
        # Try to read as plain text to show what it actually is
        with open(gz_file_path, 'rb') as f:
            content = f.read(200)
        if b'<!DOCTYPE html' in content or b'<html' in content:
            print("File appears to be HTML (probably an error page)")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()


def test_with_sample_data():
    """
    Test with sample XML data (no file needed)
    """
    print("=" * 70)
    print("TESTING WITH SAMPLE DATA")
    print("=" * 70)
    
    # Create sample XML
    sample_xml = """<?xml version="1.0" encoding="utf-8"?>
    <Prices>
        <ChainID>7290661400001</ChainID>
        <SubChainID>001</SubChainID>
        <StoreID>246</StoreID>
        <Products>
            <Product>
                <ItemCode>7290000009358</ItemCode>
                <ItemName>חלב תנובה 3%</ItemName>
                <ItemPrice>5.90</ItemPrice>
                <ManufactureName>תנובה</ManufactureName>
                <UnitQty>ליטר</UnitQty>
                <Quantity>1</Quantity>
            </Product>
            <Product>
                <ItemCode>7290000009359</ItemCode>
                <ItemName>לחם אחיד פרוס</ItemName>
                <ItemPrice>7.50</ItemPrice>
                <ManufactureName>ברמן</ManufactureName>
                <UnitQty>יחידה</UnitQty>
                <Quantity>750</Quantity>
            </Product>
        </Products>
    </Prices>"""
    
    # Create temporary gz file
    temp_file = Path("test_sample.xml.gz")
    with gzip.open(temp_file, 'wt', encoding='utf-8') as f:
        f.write(sample_xml)
    
    print("Created sample file with 2 products")
    
    # Test it
    result = test_with_local_file(str(temp_file), "test-provider", "test-branch", "pricesFull")
    
    # Clean up
    temp_file.unlink()
    
    return result


def main():
    """Main function for testing"""
    print("\n" + "🚀 EXTRACTOR LOCAL TEST SUITE 🚀".center(70))
    print("=" * 70)
    
    # Check if file argument provided
    if len(sys.argv) > 1:
        gz_file = sys.argv[1]
        
        # Optional parameters
        provider = sys.argv[2] if len(sys.argv) > 2 else "victory"
        branch = sys.argv[3] if len(sys.argv) > 3 else "tel-aviv"
        file_type = sys.argv[4] if len(sys.argv) > 4 else "pricesFull"
        
        test_with_local_file(gz_file, provider, branch, file_type)
    else:
        print("\nNo file provided. Running with sample data...")
        print("\nUsage: python test_local.py <gz_file> [provider] [branch] [type]")
        print("Example: python test_local.py /path/to/file.gz victory tel-aviv pricesFull")
        print("\n" + "-" * 70)
        
        # Run sample test
        test_with_sample_data()
        
        # Suggest testing with real files
        print("\n" + "=" * 70)
        print("To test with your actual files, run:")
        print("python test_local.py /home/amirkhalifa/Downloads/PriceFull_03082025_1_v.gz")


if __name__ == "__main__":
    main()
