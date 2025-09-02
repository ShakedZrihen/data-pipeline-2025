from typing import Iterable, Dict, Optional
import gzip, io, json, zipfile, xml.etree.ElementTree as ET

def detect_file_type(data: bytes) -> str:
    """Detect if data is ZIP, gzipped, or plain text based on magic bytes"""
    if data.startswith(b'PK'):
        return 'zip'
    elif data.startswith(b'\x1f\x8b'):
        return 'gzip'
    else:
        return 'plain'

def parse_ndjson_gz(data: bytes) -> Iterable[Dict]:
    """Parse gzipped NDJSON files"""
    with gzip.GzipFile(fileobj=io.BytesIO(data), mode="rb") as gz:
        for line in gz:
            try:
                yield json.loads(line.decode("utf-8"))
            except Exception:
                continue

def parse_xml_file(data: bytes) -> Iterable[Dict]:
    """Parse XML files and extract product information"""
    try:
        # Remove BOM if present
        if data.startswith(b'\xef\xbb\xbf'):
            data = data[3:]
        
        root = ET.fromstring(data.decode('utf-8'))
        
        # Look for Items/Item elements (common in supermarket XML)
        items = root.findall('.//Item') or root.findall('.//item') or root.findall('.//Product') or root.findall('.//product')
        
        if not items:
            # Try alternative paths
            items = root.findall('.//*[local-name()="Item"]') or root.findall('.//*[local-name()="item"]')
        
        if not items:
            print(f"No items found in XML, root tag: {root.tag}")
            return []
        
        print(f"Found {len(items)} items in XML")
        
        for item in items:
            try:
                product = {}
                
                # Extract common fields with multiple possible names
                for child in item:
                    tag = child.tag.lower()
                    text = child.text.strip() if child.text else ""
                    
                    # Product name - multiple possible field names
                    if tag in ['name', 'productname', 'itemname', 'itemnm', 'productnm']:
                        product['name'] = text
                    # Price - multiple possible field names
                    elif tag in ['price', 'itemprice', 'cost', 'itemprice']:
                        try:
                            product['price'] = float(text)
                        except:
                            product['price'] = 0.0
                    # Barcode - multiple possible field names
                    elif tag in ['barcode', 'barcod', 'ean', 'itemcode', 'productcode']:
                        product['barcode'] = text
                    # Brand - multiple possible field names
                    elif tag in ['brand', 'manufacturer', 'manufacturername', 'brandname']:
                        product['brand'] = text
                    # Category - multiple possible field names
                    elif tag in ['category', 'categoryname', 'itemtype', 'producttype']:
                        product['category'] = text
                    # Size - multiple possible field names
                    elif tag in ['size', 'quantity', 'weight', 'qtyinpackage', 'unitqty', 'unitofmeasure']:
                        product['size'] = text
                    # Promo price - multiple possible field names
                    elif tag in ['promoprice', 'saleprice', 'discountprice']:
                        try:
                            product['promo_price'] = float(text)
                        except:
                            product['promo_price'] = None
                    # Promo text - multiple possible field names
                    elif tag in ['promotext', 'saletext', 'discounttext']:
                        product['promo_text'] = text
                    # Stock status - multiple possible field names
                    elif tag in ['instock', 'available', 'itemstatus', 'availability']:
                        if text.isdigit():
                            product['in_stock'] = text == '1'
                        else:
                            product['in_stock'] = text.lower() in ['true', 'yes', 'available', 'in stock']
                    # Additional metadata
                    elif tag in ['manufacturecountry', 'manufactureritemdescription', 'priceupdatedate']:
                        product[tag] = text
                    else:
                        # Store other fields
                        product[tag] = text
                
                # Ensure required fields exist
                if 'name' in product and 'price' in product:
                    yield product
                    
            except Exception as e:
                print(f"Error parsing item: {e}")
                continue
                
    except Exception as e:
        print(f"Error parsing XML file: {e}")
        return []

def parse_zip_file(data: bytes) -> Iterable[Dict]:
    """Parse ZIP files containing XML or other data formats"""
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zip_file:
            for file_info in zip_file.filelist:
                if file_info.filename.endswith('.xml'):
                    # Extract and parse XML files
                    with zip_file.open(file_info.filename) as f:
                        content = f.read()
                        print(f"Parsing XML from ZIP: {file_info.filename}")
                        yield from parse_xml_file(content)
                elif file_info.filename.endswith('.json') or file_info.filename.endswith('.ndjson'):
                    # Extract and parse JSON/NDJSON files
                    with zip_file.open(file_info.filename) as f:
                        content = f.read().decode('utf-8')
                        for line in content.splitlines():
                            if line.strip():
                                try:
                                    yield json.loads(line)
                                except Exception:
                                    continue
                elif file_info.filename.endswith('.csv'):
                    # Extract and parse CSV files
                    with zip_file.open(file_info.filename) as f:
                        content = f.read().decode('utf-8')
                        lines = content.splitlines()
                        if lines:
                            headers = lines[0].split(',')
                            for line in lines[1:]:
                                if line.strip():
                                    try:
                                        values = line.split(',')
                                        row = dict(zip(headers, values))
                                        yield row
                                    except Exception:
                                        continue
    except Exception as e:
        print(f"Error parsing ZIP file: {e}")
        return []

def parse_ndjson_plain(data: bytes) -> Iterable[Dict]:
    """Parse plain NDJSON files"""
    try:
        content = data.decode('utf-8')
        for line in content.splitlines():
            if line.strip():
                try:
                    yield json.loads(line)
                except Exception:
                    continue
    except Exception as e:
        print(f"Error parsing plain NDJSON: {e}")
        return []

def parse_by_filename(key: str, data: bytes) -> Optional[Iterable[Dict]]:
    """
    Smart parser that detects file type and uses appropriate parsing method.
    Returns an iterator of product dicts, or None if unsupported.
    """
    file_type = detect_file_type(data)
    
    if file_type == 'zip':
        print(f"Detected ZIP file: {key}")
        return parse_zip_file(data)
    elif file_type == 'gzip':
        print(f"Detected gzipped file: {key}")
        # Check if it's XML content inside gzip
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(data), mode="rb") as gz:
                content = gz.read()
                if content.startswith(b'<') or content.startswith(b'\xef\xbb\xbf<'):
                    print(f"Gzipped file contains XML: {key}")
                    return parse_xml_file(content)
                else:
                    print(f"Gzipped file contains other content: {key}")
                    return parse_ndjson_gz(data)
        except Exception as e:
            print(f"Error checking gzipped content: {e}")
            return parse_ndjson_gz(data)
    elif file_type == 'plain':
        print(f"Detected plain text file: {key}")
        return parse_ndjson_plain(data)
    else:
        print(f"Unknown file type for: {key}")
        return None
