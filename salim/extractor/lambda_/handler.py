import os, json, boto3, tempfile
from http.server import HTTPServer, BaseHTTPRequestHandler
from botocore.exceptions import ClientError

from ..utils.file_utils import extract_and_delete_gz, convert_xml_to_json
from ..utils.convert_json_format import (
    convert_json_to_target_prices_format,
    convert_json_to_target_promos_format,)
from ..utils.send_json_to_sqs import send_promotions_in_chunks, send_items_in_chunks

TMP_DIR = os.getenv("TMP_DIR", tempfile.gettempdir())
os.makedirs(TMP_DIR, exist_ok=True)

BASE_DIR = os.path.dirname(__file__)
lookup_path = os.path.join(BASE_DIR, "utils", "stores_lookup.json")

def _to_tmp_path(object_key: str) -> str:
    return os.path.join(TMP_DIR, os.path.basename(object_key))

def lambda_handler(event, context=None):
    """AWS Lambda handler for S3 events"""
    print(f"Received event: {json.dumps(event, indent=2)}")
    
    s3_client = boto3.client(
    's3',
    endpoint_url=os.getenv('S3_ENDPOINT', 'http://localhost:4566'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
    region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
    
    try:
        if 'Records' not in event:
            return {'statusCode': 200, 'body': json.dumps('No records')}
    
        if 'Records' in event:
            for record in event['Records']:
                bucket_name = record['s3']['bucket']['name']
                object_key = record['s3']['object']['key']
                event_name = record['eventName']
                
                print(f"🎯 S3 Event Triggered!")
                print(f"   Event: {event_name}")
                print(f"   Bucket: {bucket_name}")
                print(f"   File: {object_key}")
                
                try:
                    head = s3_client.head_object(Bucket=bucket_name, Key=object_key)
                    print(f"   Size: {head.get('ContentLength')} bytes")
                    print(f"   Modified: {head.get('LastModified')}")
                except ClientError as e:
                    print(f"head_object failed (continuing): {e}")

                tmp_path = _to_tmp_path(object_key)
                s3_client.download_file(bucket_name, object_key, tmp_path)
                print(f"Saved object to {tmp_path}")
                
                if object_key.lower().endswith(".gz"):
                    xml_path = extract_and_delete_gz(tmp_path) 
                else:
                    xml_path = tmp_path
                    
                json_intermediate_path = convert_xml_to_json(xml_path)
                print(f"Intermediate JSON: {json_intermediate_path}")

                with open(json_intermediate_path, "r", encoding="utf-8") as jf:
                    intermediate_obj = json.load(jf)

                root = intermediate_obj.get("Root") or intermediate_obj.get("root") or {}
                is_prices = ("Items" in root) or ("items" in root)
                is_promos = ("Promotions" in root) or ("promotions" in root)

                base_no_ext = os.path.splitext(os.path.basename(json_intermediate_path))[0]
                target_json_path = os.path.join(TMP_DIR, f"{base_no_ext}_converted.json")

                if is_prices:
                    convert_json_to_target_prices_format(json_intermediate_path, target_json_path, stores_lookup_path=lookup_path)
                elif is_promos:
                    convert_json_to_target_promos_format(json_intermediate_path, target_json_path, stores_lookup_path=lookup_path)
                else:
                    print("Unknown JSON structure (no Items/Promotions). Skipping.")
                    continue

                print(f"Target JSON: {target_json_path}")

                with open(target_json_path, "r", encoding="utf-8") as tf:
                    target_dict = json.load(tf)

                try:
                    if is_prices:
                        send_items_in_chunks(target_dict, limit=200)
                    elif is_promos:
                        send_promotions_in_chunks(target_dict)
                    else:
                        raise ValueError("Unrecognized structure — no 'items' or 'promotions' found")
                except Exception as ve:
                    print(f"Failed to send chunks to SQS: {ve}")
                
                for p in [tmp_path, json_intermediate_path, target_json_path]:
                    try:
                        if p and os.path.exists(p):
                            os.remove(p)
                    except Exception as e:
                        print(f"cleanup failed for {p}: {e}")

                print(f"Sent {object_key} to SQS successfully")
                print("-" * 50)
        else:
            print("No S3 records found in event")
            
    except Exception as e:
        print(f"Error processing event: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
    
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function executed successfully')
    }

class LambdaHTTPHandler(BaseHTTPRequestHandler):
    """HTTP handler to simulate Lambda invocation"""
    
    def do_GET(self):
        """Handle GET requests to list S3 files or download all files"""
        try:
            if self.path == '/files':
                s3_client = boto3.client(
                    's3',
                    endpoint_url=os.getenv('S3_ENDPOINT', 'http://localhost:4566'),
                    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
                    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
                    region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
                )
                
                bucket_name = os.getenv('S3_BUCKET', 'test-bucket')
                
                try:
                    response = s3_client.list_objects_v2(Bucket=bucket_name)
                    files = []
                    
                    if 'Contents' in response:
                        for obj in response['Contents']:
                            files.append({
                                'key': obj['Key'],
                                'size': obj['Size'],
                                'lastModified': obj['LastModified'].isoformat(),
                                'etag': obj['ETag']
                            })
                    
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        'files': files,
                        'bucket': bucket_name
                    }).encode('utf-8'))
                    
                except ClientError as e:
                    self.send_response(404)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
                    
            elif self.path == '/download-all':
                self.download_all_files_from_s3()
                
            else:
                self.send_response(404)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'Not found'}).encode('utf-8'))
                
        except Exception as e:
            print(f"Error handling GET request: {e}")
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
    
    def do_POST(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length > 0:
                body = self.rfile.read(content_length).decode('utf-8')
                event = json.loads(body)
            else:
                event = {}
            
            response = lambda_handler(event)
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode('utf-8'))
            
        except Exception as e:
            print(f"Error handling request: {e}")
            self.send_response(500)
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
    
    def do_OPTIONS(self):
        """Handle preflight CORS requests"""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def log_message(self, format, *args):
        # Suppress default HTTP server logs
        pass

    def download_all_files_from_s3(self):
        """Download all files from S3 maintaining directory structure"""
        try:
            s3_client = boto3.client(
                's3',
                endpoint_url=os.getenv('S3_ENDPOINT', 'http://localhost:4566'),
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
                region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
            )
            
            bucket_name = os.getenv('S3_BUCKET', 'test-bucket')
            current_dir = os.getcwd()
            download_dir = os.path.join(current_dir, 'downloaded_files')
            
            # Create base download directory
            os.makedirs(download_dir, exist_ok=True)
            
            # Get all objects from S3
            paginator = s3_client.get_paginator('list_objects_v2')
            downloaded_count = 0
            
            for page in paginator.paginate(Bucket=bucket_name):
                for obj in page.get('Contents', []):
                    s3_key = obj['Key']
                    
                    # Create local path maintaining S3 directory structure
                    local_path = os.path.join(download_dir, s3_key.replace('/', os.sep))
                    local_dir = os.path.dirname(local_path)
                    
                    # Create directory if it doesn't exist
                    os.makedirs(local_dir, exist_ok=True)
                    
                    # Download file
                    try:
                        s3_client.download_file(bucket_name, s3_key, local_path)
                        downloaded_count += 1
                        print(f"📥 Downloaded: {s3_key} -> {local_path}")
                    except Exception as e:
                        print(f"❌ Failed to download {s3_key}: {e}")
            
            response_data = {
                'message': f'Successfully downloaded {downloaded_count} files',
                'download_directory': download_dir,
                'total_files': downloaded_count
            }
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(response_data).encode('utf-8'))
            
        except Exception as e:
            print(f"Error downloading files: {e}")
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))

def main():
    """Start HTTP server to receive Lambda events and polling loop"""
    port = int(os.getenv('LAMBDA_PORT', 8080))
    print(f"🚀 Lambda function server starting on port {port}...")
    
    server = HTTPServer(('0.0.0.0', port), LambdaHTTPHandler)
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down lambda function...")
        server.shutdown()

if __name__ == "__main__":
    main()