import boto3
import os
import json
import time
import threading
import sys
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
                    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
                    body = response['Body'].read().decode('utf-8')
                    size = response['ContentLength']
                    modified = response['LastModified']
                    print(f"Retrieved file {object_key} from S3: {body[:100]}...")
                    print(f"   Size: {size} bytes")
                    print(f"   Modified: {modified}")

                    # Parse the body as JSON
                    json_data = json.loads(body)

                    # Send to SQS
                    send_json_to_sqs(json_data)
                except ClientError as e:
                    print(f"   Error getting object details: {e}")
                
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
        """Handle GET requests to list S3 files"""
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
            else:
                self.send_response(404)
                self.end_headers()
                
        except Exception as e:
            print(f"Error handling GET request: {e}")
            self.send_response(500)
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