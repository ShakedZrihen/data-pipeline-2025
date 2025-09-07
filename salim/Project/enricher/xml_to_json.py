import os
import json
import argparse
import xml.etree.ElementTree as ET
from typing import Optional


def convert_xml_to_json(xml_file_path: str) -> Optional[str]:
	"""
	Converts an XML file (even if extensionless) to a JSON file.
	Skips conversion if the JSON file already exists.
	Returns the path to the created/existing JSON file, or None on error.
	"""
	json_file_path = xml_file_path + ".json"
	if os.path.exists(json_file_path):
		print(f"JSON already exists: {json_file_path}")
		return json_file_path

	try:
		# Parse XML directly from file to handle BOM/encoding automatically
		tree = ET.parse(xml_file_path)
		root = tree.getroot()

		# Convert recursively
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

		# Save to JSON
		with open(json_file_path, "w", encoding="utf-8") as json_file:
			json.dump(parsed_dict, json_file, ensure_ascii=False, indent=2)

		print(f"Converted to JSON: {json_file_path}")
		return json_file_path
	except Exception as e:
		print(f"Error converting {xml_file_path} to JSON: {e}")
		return None


def main():
	parser = argparse.ArgumentParser(description="Convert an XML file to JSON next to it (adds .json suffix)")
	parser.add_argument("xml_path", help="Path to the XML file")
	args = parser.parse_args()

	xml_path = args.xml_path
	if not os.path.exists(xml_path):
		print(f"File not found: {xml_path}")
		exit(1)

	json_path = convert_xml_to_json(xml_path)
	if not json_path:
		exit(1)

	print(json_path)


if __name__ == "__main__":
	main()


