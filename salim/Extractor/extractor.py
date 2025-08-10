import os
import boto3
import pandas as pd
import json
import xml.etree.ElementTree as ET
from io import StringIO

def get_s3_client():
    """
    Returns a boto3 S3 client configured for LocalStack.
    """
    return boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
class Extractor:
    """
    The Extracor will only extract data from the S3 bucket and convert it to a unified JSON format.
    Later the unifed_data will tkae this .json file and will unifed the attributes of the files.
    """
    def __init__(self):
        self.s3_client = get_s3_client()
        self.s3_bucket = 'test-bucket'
        self.s3_prefix = ""  # Empty prefix to get all files
        self.s3_key = "test"
  
    def extract_data(self):
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.s3_bucket, Prefix=self.s3_prefix)
            files = []
            for obj in response.get('Contents', []):
                files.append(obj['Key'])
                print(f"Found file: {obj['Key']}")
            return files
        except Exception as e:
            print(f"Error extracting data: {e}")
            return []

    def download_and_convert_to_json(self):
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.s3_bucket, Prefix=self.s3_prefix)
            all_data = []
            
            for obj in response.get('Contents', []):
                file_key = obj['Key']
                print(f"Processing file: {file_key}")
                file_obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=file_key)
                file_content = file_obj['Body'].read().decode('utf-8')
                json_data = self.convert_file_to_json(file_content, file_key)
                
                if json_data:
                    all_data.append({
                        'file_name': file_key,
                        'data': json_data,
                        'file_size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat()
                    })
            
            return all_data
        except Exception as e:
            print(f"Error downloading and converting files: {e}")
            return []

    def convert_file_to_json(self, content, filename):
        try:
            # If file is already JSON
            if filename.lower().endswith('.json'):
                return json.loads(content)
            
            # If file is XML
            elif filename.lower().endswith('.xml') or 'xml' in content.lower()[:100]:
                return self.xml_to_json(content)
            
            # If file is CSV
            elif filename.lower().endswith('.csv') or ',' in content:
                return self.csv_to_json(content)
            
            # If file is plain text, try to parse as structured data
            else:
                return self.text_to_json(content, filename)
                
        except Exception as e:
            print(f"Error converting {filename}: {e}")
            return None

    def xml_to_json(self, xml_content):

        try:
            root = ET.fromstring(xml_content)
            return self.xml_element_to_dict(root)
        except Exception as e:
            print(f"Error parsing XML: {e}")
            return {"raw_content": xml_content}

    def xml_element_to_dict(self, element):
        result = {}
        
        # Add attributes
        if element.attrib:
            result['@attributes'] = element.attrib
        
        if element.text and element.text.strip():
            if len(element) == 0:
                return element.text
            result['text'] = element.text.strip()
        
        for child in element:
            child_data = self.xml_element_to_dict(child)
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        
        return result
    def csv_to_json(self, csv_content):
        """Convert CSV content to JSON"""
        try:
            df = pd.read_csv(StringIO(csv_content))
            return df.to_dict('records')
        except Exception as e:
            print(f"Error parsing CSV: {e}")
            return {"raw_content": csv_content}

    def text_to_json(self, content, filename):
        """Convert text content to JSON structure"""
        try:
            lines = content.strip().split('\n')
            return {
                'filename': filename,
                'line_count': len(lines),
                'content': lines,
                'raw_content': content
            }
        except Exception as e:
            print(f"Error parsing text: {e}")
            return {"raw_content": content}

    def create_unified_json(self):
        """Create a unified JSON structure from all files"""
        all_data = self.download_and_convert_to_json()
        
        unified_data = {
            'extraction_timestamp': pd.Timestamp.now().isoformat(),
            'total_files': len(all_data),
            'files': all_data,
            'summary': {
                'file_types': {},
                'total_size': 0
            }
        }
        
        for file_data in all_data:
            file_ext = file_data['file_name'].split('.')[-1].lower() if '.' in file_data['file_name'] else 'unknown'
            unified_data['summary']['file_types'][file_ext] = unified_data['summary']['file_types'].get(file_ext, 0) + 1
            unified_data['summary']['total_size'] += file_data['file_size']
        
        return unified_data

    def save_unified_json(self, output_file='unified_data.json'):
        unified_data = self.create_unified_json()
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(unified_data, f, indent=2, ensure_ascii=False)
        print(f"Unified JSON saved to: {output_file}")
        return unified_data
        
    def empty_s3_bucket(self):
        """Empty the S3 bucket"""
        self.s3_client.delete_bucket(Bucket=self.s3_bucket)
        self.s3_client.create_bucket(Bucket=self.s3_bucket)

if __name__ == "__main__":
    extractor = Extractor()
    extractor.save_unified_json('unified_data.json')

