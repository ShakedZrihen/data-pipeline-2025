import boto3
import os
import json
import time
import threading
import sys
import io
import gzip
from http.server import HTTPServer, BaseHTTPRequestHandler
from botocore.exceptions import ClientError

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.send_json_to_sqs import send_json_to_sqs

def lambda_handler(event, context=None):
    """AWS Lambda handler for S3 events"""
    print(f"Received event: {json.dumps(event, indent=2)}")
    
    s3_client = boto3.client(
    's3',
    endpoint_url=os.getenv('S3_ENDPOINT', 'http://localhost:4566'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
    region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
)
    
    try:
        if 'Records' in event:
            for record in event['Records']:
                bucket_name = record['s3']['bucket']['name']
                object_key = record['s3']['object']['key']
                event_name = record['eventName']
                
                print(f"🎯 S3 Event Triggered!")
                print(f"   Event: {event_name}")
                print(f"   Bucket: {bucket_name}")
                print(f"   File: {object_key}")
                
                # Get object details
                try:
                    obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
                    body_bytes = obj['Body'].read()
                    size = len(body_bytes)
                    modified = obj.get('LastModified')
                    print(f"   Size: {size} bytes")
                    print(f"   Modified: {modified}")

                    # if it is .gz - extract it
                    if object_key.lower().endswith('.gz'):
                        with gzip.GzipFile(fileobj=io.BytesIO(body_bytes)) as gz:
                            payload_str = gz.read().decode('utf-8')
                        print("Decompressed .gz successfully")
                    else:
                        payload_str = body_bytes.decode('utf-8')
                except ClientError as e:
                    print(f"Error getting object details: {e}")
                
                # *****************

                # 1. payload_str is the extracted .gz (xml currently). So you need implement the conversion to json and parse the data.
                # 2. below you can see the sending to SQS. Define json_content.

                # *****************
                
                # conver to dict
                json_content = json.loads(payload_str)

                # send json to sqs
                send_json_to_sqs(json_content)

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