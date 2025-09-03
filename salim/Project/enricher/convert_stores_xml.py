import os
import glob
from xml_to_json import convert_xml_to_json

def convert_all_stores_xml():
    """Convert all XML files in the Stores folder to JSON"""
    
    stores_dir = "Stores"
    
    # Check if Stores directory exists
    if not os.path.exists(stores_dir):
        print(f"Stores directory not found: {stores_dir}")
        return
    
    # Find all XML files (including those without .xml extension)
    xml_files = []
    
    # Look for files with .xml extension
    xml_files.extend(glob.glob(os.path.join(stores_dir, "*.xml")))
    
    # Look for files without extension that might be XML
    all_files = os.listdir(stores_dir)
    for file in all_files:
        file_path = os.path.join(stores_dir, file)
        if os.path.isfile(file_path) and '.' not in file:
            # Check if it's an XML file by reading first few characters
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    if first_line.startswith('<?xml') or first_line.startswith('<'):
                        xml_files.append(file_path)
            except:
                pass
    
    print(f"Found {len(xml_files)} XML files in {stores_dir}:")
    for xml_file in xml_files:
        print(f"  • {os.path.basename(xml_file)}")
    
    print(f"\nConverting XML files to JSON...")
    
    converted_count = 0
    for xml_file in xml_files:
        print(f"\nProcessing: {os.path.basename(xml_file)}")
        json_path = convert_xml_to_json(xml_file)
        if json_path:
            converted_count += 1
            print(f"Success: {os.path.basename(json_path)}")
        else:
            print(f"Failed to convert: {os.path.basename(xml_file)}")
    
    print(f"\nConversion complete! {converted_count}/{len(xml_files)} files converted successfully.")

if __name__ == "__main__":
    convert_all_stores_xml()
