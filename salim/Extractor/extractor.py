import os
import boto3
import pandas as pd
import json
import xml.etree.ElementTree as ET
from io import StringIO

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

class Extractor:
    def __init__(self):
        self.s3_client = get_s3_client()
        self.s3_bucket = 'test-bucket'
        self.s3_prefix = ""  
        self.s3_key = "test"
        self.output_dir = "extracted_files"

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

    def convert_file_to_json(self, content, filename):
        try:
        
            if filename.lower().endswith('.json'):
                return json.loads(content)
            
 
            elif filename.lower().endswith('.xml') or 'xml' in content.lower()[:100]:
                return self.xml_to_json(content)
            

            elif filename.lower().endswith('.csv') or ',' in content:
                return self.csv_to_json(content)
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

    def create_json_of_object(self, obj):
        file_key = obj['Key']
        print(f"Processing file: {file_key}")
        
        file_obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=file_key)
        file_content = file_obj['Body'].read().decode('utf-8')
        
        json_data = self.convert_file_to_json(file_content, file_key)
        
        if json_data:
            file_data = {
                'action': 'process_json_file',
                'original_filename': file_key,
                'timestamp': pd.Timestamp.now().isoformat(),
                'file_size': obj['Size'],
                'last_modified': obj['LastModified'].isoformat(),
                'data': json_data
            }

            return file_data

    def create_individual_json_files(self, callback=None):
        """Create individual JSON files for each object"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"Created output directory: {self.output_dir}")
        
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.s3_bucket, Prefix=self.s3_prefix)
            processed_files = []
            
            for obj in response.get('Contents', []):
                if obj['Size'] == 0:
                    print(f"Skipping empty file: {obj['Key']}")
                    continue
                file_info = self.create_json_of_object(obj)
                callback(file_info) if callback else None
                processed_files.append(obj['Key'])

            summary = {
                'extraction_timestamp': pd.Timestamp.now().isoformat(),
                'total_files_processed': len(processed_files),
                'output_directory': self.output_dir,
                'files': processed_files
            }
            
            summary_path = os.path.join(self.output_dir, 'extraction_summary.json')
            with open(summary_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            
            print(f"Summary saved to: {summary_path}")
            print(f"Total files processed: {len(processed_files)}")
            
            return processed_files
            
        except Exception as e:
            print(f"Error creating individual JSON files: {e}")
            return []

    def create_safe_filename(self, filename):
        name = filename.split('.')[0] if '.' in filename else filename
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            name = name.replace(char, '_')
        name = name.replace(' ', '_').replace('.', '_')
        if len(name) > 100:
            name = name[:100]
        return name
        
    def empty_s3_bucket(self):
        self.s3_client.delete_bucket(Bucket=self.s3_bucket)
        self.s3_client.create_bucket(Bucket=self.s3_bucket)

