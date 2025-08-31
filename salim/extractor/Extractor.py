import os
import boto3
import pandas as pd
import json
import xml.etree.ElementTree as ET
from io import StringIO
from typing import List, Dict, Any, Optional


def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )


class Extractor:
    """Extractor class for processing files from S3 and converting them to JSON format."""
    
    def __init__(self, bucket_name: str = 'test-bucket', prefix: str = "", output_dir: str = "extracted_files"):
        """
        Initialize the Extractor with S3 configuration.
        
        Args:
            bucket_name: Name of the S3 bucket
            prefix: S3 key prefix for filtering objects
            output_dir: Directory to save extracted JSON files
        """
        self.s3_client = get_s3_client()
        self.s3_bucket = bucket_name
        self.s3_prefix = prefix
        self.output_dir = output_dir

    def list_s3_files(self) -> List[str]:
        """
        List all files in the S3 bucket with the specified prefix.
        
        Returns:
            List of file keys found in the bucket
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket, 
                Prefix=self.s3_prefix
            )
            files = []
            for obj in response.get('Contents', []):
                files.append(obj['Key'])
                print(f"Found file: {obj['Key']}")
            return files
        except Exception as e:
            print(f"Error listing S3 files: {e}")
            return []

    def extract_data(self) -> List[str]:
        return self.list_s3_files()

    def convert_file_to_json(self, content: str, filename: str) -> Optional[Dict[str, Any]]:
        try:
            if filename.lower().endswith('.json'):
                return json.loads(content)
            
            elif filename.lower().endswith('.xml') or 'xml' in content.lower()[:100]:
                return self._xml_to_json(content)
            
            elif filename.lower().endswith('.csv') or ',' in content:
                return self._csv_to_json(content)
            
            else:
                return self._text_to_json(content, filename)
                
        except Exception as e:
            print(f"Error converting {filename}: {e}")
            return None

    def _xml_to_json(self, xml_content: str) -> Dict[str, Any]:
        """Convert XML content to JSON format."""
        try:
            root = ET.fromstring(xml_content)
            return self._xml_element_to_dict(root)
        except Exception as e:
            print(f"Error parsing XML: {e}")
            return {"raw_content": xml_content}

    def _xml_element_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """Recursively convert XML element to dictionary."""
        result = {}
        
        if element.attrib:
            result['@attributes'] = element.attrib
        
        if element.text and element.text.strip():
            if len(element) == 0:
                return element.text
            result['text'] = element.text.strip()
        
        for child in element:
            child_data = self._xml_element_to_dict(child)
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        
        return result

    def _csv_to_json(self, csv_content: str) -> List[Dict[str, Any]]:
        """Convert CSV content to JSON format."""
        try:
            df = pd.read_csv(StringIO(csv_content))
            return df.to_dict('records')
        except Exception as e:
            print(f"Error parsing CSV: {e}")
            return {"raw_content": csv_content}

    def _text_to_json(self, content: str, filename: str) -> Dict[str, Any]:
        """Convert text content to JSON structure."""
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

    def _create_filename(self, filename: str) -> str:
        """Create a filename by removing invalid characters."""
        name = filename.split('.')[0] if '.' in filename else filename
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            name = name.replace(char, '_')
        name = name.replace(' ', '_').replace('.', '_')
        if len(name) > 100:
            name = name[:100]
        return name

    def _ensure_output_directory(self) -> None:
        """Create output directory if it doesn't exist."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"Created output directory: {self.output_dir}")

    def _process_single_file(self, obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single S3 object and convert it to JSON."""
        file_key = obj['Key']
        print(f"Processing file: {file_key}")
        
        try:
            file_obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=file_key)
            file_content = file_obj['Body'].read().decode('utf-8')
            
            json_data = self.convert_file_to_json(file_content, file_key)
            
            if json_data:
                safe_filename = self._create_filename(file_key)
                output_path = os.path.join(self.output_dir, f"{safe_filename}.json")
                
                file_data = {
                    'original_filename': file_key,
                    'extraction_timestamp': pd.Timestamp.now().isoformat(),
                    'file_size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                    'data': json_data
                }
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(file_data, f, indent=2, ensure_ascii=False)
                
                print(f"Saved: {output_path}")
                
                return {
                    'original_key': file_key,
                    'json_file': output_path,
                    'size': obj['Size']
                }
        except Exception as e:
            print(f"Error processing file {file_key}: {e}")
            return None

    def create_json_files(self) -> List[Dict[str, Any]]:
        """Process all files in S3 bucket and create individual JSON files."""
        self._ensure_output_directory()
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket, 
                Prefix=self.s3_prefix
            )
            processed_files = []
            
            for obj in response.get('Contents', []):
                result = self._process_single_file(obj)
                if result:
                    processed_files.append(result)
            
            self._create_summary_file(processed_files)
            
            print(f"Total files processed: {len(processed_files)}")
            return processed_files
            
        except Exception as e:
            print(f"Error creating individual JSON files: {e}")
            return []

    def _create_summary_file(self, processed_files: List[Dict[str, Any]]) -> None:
        """Create a summary file with processing information."""
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

    def empty_s3_bucket(self) -> None:
        """Delete and recreate the S3 bucket to empty it."""
        try:
            self.s3_client.delete_bucket(Bucket=self.s3_bucket)
            self.s3_client.create_bucket(Bucket=self.s3_bucket)
            print(f"Emptied bucket: {self.s3_bucket}")
        except Exception as e:
            print(f"Error emptying bucket: {e}")
