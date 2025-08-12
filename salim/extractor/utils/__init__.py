import gzip
import json
import os
import shutil
import xml.etree.ElementTree as ET

import requests
import xmltodict


def extract_and_delete_gz(gz_path, folder_path):
    if not gz_path.endswith(".gz"):
        print("Not a .gz file:", gz_path)
        return None

    # Determine output filename by removing ".gz"
    fname = os.path.basename(gz_path)[:-3] + ".xml"
    os.makedirs(folder_path, exist_ok=True)

    output_path = os.path.join(folder_path, fname)

    # Extract the .gz file
    with gzip.open(gz_path, "rb") as f_in:
        with open(output_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    print(f"Extracted to: {output_path}")

    # Delete the original .gz file
    os.remove(gz_path)
    print(f"Deleted: {gz_path}")
    return output_path


def download_file_from_link(link, output_dir):
    filename = os.path.basename(link)
    output_path = os.path.join(output_dir, filename)
    response = requests.get(link, stream=True)
    if response.status_code == 200:
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded to {output_path}")
        return output_path
    else:
        print(f"Failed to download. Status code: {response.status_code}")
        return None


def xml_file_to_json(path, as_str=False):
    with open(path, "r", encoding="utf-8") as f:
        obj = xmltodict.parse(f.read())
    return json.dumps(obj, ensure_ascii=False, indent=2) if as_str else obj


def convert_xml_to_json(xml_file_path: str):
    """
    Converts an XML file (even if extensionless) to a JSON file.
    Skips conversion if the JSON file already exists.
    """
    json_file_path = xml_file_path + ".json"
    if os.path.exists(json_file_path):
        print(f"✅ JSON already exists: {json_file_path}")
        return json_file_path

    # Step 1: Read XML
    with open(xml_file_path, "r", encoding="utf-8") as f:
        xml_data = f.read()

    # Step 2: Parse XML
    root = ET.fromstring(xml_data)

    # Step 3: Convert recursively
    def elem_to_dict(elem):
        result = {elem.tag: {} if elem.attrib else None}
        children = list(elem)
        if children:
            dd = {}
            for dc in map(elem_to_dict, children):
                for k, v in dc.items():
                    if k in dd:
                        if not isinstance(dd[k], list):
                            dd[k] = [dd[k]]
                        dd[k].append(v)
                    else:
                        dd[k] = v
            result = {elem.tag: dd}
        if elem.attrib:
            result[elem.tag].update(("@" + k, v) for k, v in elem.attrib.items())
        if elem.text and elem.text.strip():
            text = elem.text.strip()
            if children or elem.attrib:
                result[elem.tag]["#text"] = text
            else:
                result[elem.tag] = text
        return result

    parsed_dict = elem_to_dict(root)

    # Step 4: Save to JSON
    with open(json_file_path, "w", encoding="utf-8") as json_file:
        json.dump(parsed_dict, json_file, ensure_ascii=False, indent=2)

    print(f"✅ Converted to JSON: {json_file_path}")
