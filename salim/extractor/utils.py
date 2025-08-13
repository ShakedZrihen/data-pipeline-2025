import gzip
import json
import shutil
import zipfile
import os
import re
import xml.etree.ElementTree as ET

_INVALID_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1F]')

def sanitize_path_component(name: str) -> str:
    """Keep Unicode (incl. Hebrew), remove only illegal filesystem chars."""
    if not isinstance(name, str):
        name = str(name)
    name = _INVALID_CHARS.sub("_", name).strip()
    return name.rstrip(" .")

def extract_and_delete_gz(path: str, delete_gz: bool = False):
    """Extract gzip OR zip (auto-detect by magic bytes). Return extracted XML path, or None."""
    # Read first 4 bytes to detect format
    with open(path, "rb") as fh:
        sig = fh.read(4)

    # GZIP: 1F 8B
    if sig.startswith(b"\x1f\x8b"):
        out_path = os.path.splitext(path)[0]  # drop .gz
        with gzip.open(path, "rb") as src, open(out_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
        if delete_gz:
            try:
                os.remove(path)
            except OSError:
                pass
        return out_path

    # ZIP: "PK"
    if sig.startswith(b"PK"):
        # extract the largest .xml in the zip (most archives contain a single xml)
        with zipfile.ZipFile(path) as zf:
            xml_members = [m for m in zf.namelist() if m.lower().endswith(".xml")]
            if not xml_members:
                gz_members = [m for m in zf.namelist() if m.lower().endswith(".gz")]
                if gz_members:
                    extract_dir = os.path.dirname(path)
                    inner_gz = zf.extract(gz_members[0], path=extract_dir)
                    return extract_and_delete_gz(inner_gz, delete_gz=True)
                print("Zip has no XML; skipping.")
                return None

            member = max(xml_members, key=lambda m: zf.getinfo(m).file_size)
            extract_dir = os.path.dirname(path)
            extracted_full = zf.extract(member, path=extract_dir)

            # Flatten if it was inside folders
            out_path = os.path.join(extract_dir, os.path.basename(member))
            if extracted_full != out_path:
                os.makedirs(os.path.dirname(out_path), exist_ok=True)
                try:
                    os.replace(extracted_full, out_path)
                except OSError:
                    shutil.copy2(extracted_full, out_path)

            if delete_gz:
                try:
                    os.remove(path)
                except OSError:
                    pass
            return out_path
        
    print("Unknown file format (not gzip/zip).")
    return None

def convert_xml_to_json(xml_file_path: str) -> str | None:
    """
    Converts an XML file (even if extensionless) to a JSON file.
    Skips conversion if the JSON file already exists.
    """
    if not os.path.exists(xml_file_path):
        print(f"XML not found: {xml_file_path}")
        return None

    json_file_path = xml_file_path + ".json"
    if os.path.exists(json_file_path):
        print(f"JSON already exists: {json_file_path}")
        return json_file_path

    with open(xml_file_path, "r", encoding="utf-8") as f:
        xml_data = f.read()

    root = ET.fromstring(xml_data)

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

    with open(json_file_path, "w", encoding="utf-8") as json_file:
        json.dump(parsed_dict, json_file, ensure_ascii=False, indent=2)

    print(f"Converted to JSON: {json_file_path}")
    return json_file_path
