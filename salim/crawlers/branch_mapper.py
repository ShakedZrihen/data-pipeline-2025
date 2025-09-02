import os
import xml.etree.ElementTree as ET
from pathlib import Path


class BranchMapper:
    def __init__(self):
        self.branches_dir = os.path.join(os.path.dirname(__file__), "Branches")
        self.branch_cache = {}  # Cache parsed XML data
    
    def get_branch_name(self, supermarket_name, branch_id):
        """Get branch name from XML file based on supermarket name and branch ID"""
        try:
            # Convert branch_id to string and remove leading zeros for comparison
            branch_id_str = str(branch_id).lstrip('0') or '0'
            
            # Load XML data if not cached
            if supermarket_name not in self.branch_cache:
                self._load_xml_data(supermarket_name)
            
            # Look up branch name
            if supermarket_name in self.branch_cache:
                store_mapping = self.branch_cache[supermarket_name]
                
                # Try exact match first
                if branch_id_str in store_mapping:
                    return store_mapping[branch_id_str]
                
                # Try with original branch_id (with leading zeros)
                if str(branch_id) in store_mapping:
                    return store_mapping[str(branch_id)]
                
                print(f"Branch ID {branch_id} not found in {supermarket_name}.xml")
                return None
            
            print(f"No XML data loaded for {supermarket_name}")
            return None
            
        except Exception as e:
            print(f"Error getting branch name for {supermarket_name}, branch {branch_id}: {e}")
            return None
    
    def _load_xml_data(self, supermarket_name):
        """Load and parse XML file for a supermarket"""
        try:
            # Construct XML filename (capitalize first letter)
            xml_filename = f"{supermarket_name.capitalize()}.xml"
            xml_path = os.path.join(self.branches_dir, xml_filename)
            
            print(f"Loading branch data from: {xml_path}")
            
            if not os.path.exists(xml_path):
                print(f"XML file not found: {xml_path}")
                return
            
            # Parse XML with UTF-8 encoding
            with open(xml_path, 'r', encoding='utf-8') as file:
                content = file.read()
            
            # Remove problematic first line if it exists
            if content.startswith('This XML file does not appear'):
                lines = content.split('\n')
                content = '\n'.join(lines[1:])  # Skip first line
            
            root = ET.fromstring(content)
            
            # Extract store mappings
            store_mapping = {}
            
            # Handle different root element names
            stores = root.findall('.//Store')  # Find all Store elements anywhere in the document
            
            for store in stores:
                store_id_elem = store.find('StoreID')
                store_name_elem = store.find('StoreName')
                
                if store_id_elem is not None and store_name_elem is not None:
                    store_id = store_id_elem.text.strip() if store_id_elem.text else None
                    store_name = store_name_elem.text.strip() if store_name_elem.text else None
                    
                    if store_id and store_name:
                        # Use original Hebrew store name as folder name
                        
                        # Store with different formats of store_id
                        store_mapping[store_id] = store_name
                        store_mapping[store_id.lstrip('0') or '0'] = store_name
                        # Also store with leading zeros padded to 3 digits
                        padded_id = store_id.zfill(3)
                        store_mapping[padded_id] = store_name
            
            self.branch_cache[supermarket_name] = store_mapping
            print(f"Loaded {len(store_mapping)} store mappings for {supermarket_name}")
            print(f"Available Store IDs: {list(set(k for k in store_mapping.keys() if not k.startswith('0') or k == '0'))}")
            
        except Exception as e:
            print(f"Error loading XML data for {supermarket_name}: {e}")
            self.branch_cache[supermarket_name] = {}
    
    def get_all_branches(self, supermarket_name):
        """Get all available branches for a supermarket"""
        try:
            if supermarket_name not in self.branch_cache:
                self._load_xml_data(supermarket_name)
            
            if supermarket_name in self.branch_cache:
                return list(self.branch_cache[supermarket_name].values())
            
            return []
            
        except Exception as e:
            print(f"Error getting all branches for {supermarket_name}: {e}")
            return []