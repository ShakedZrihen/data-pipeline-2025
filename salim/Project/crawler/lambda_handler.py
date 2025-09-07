import boto3
import os
import json
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from botocore.exceptions import ClientError

class LambdaHTTPHandler(BaseHTTPRequestHandler):
    """HTTP handler to serve S3 file listing API"""
    
    def do_GET(self):
        """Handle GET requests to list S3 files"""
        try:
            if self.path == '/files':
                s3_client = boto3.client(
                    's3',
                    endpoint_url=os.getenv('S3_ENDPOINT', 'http://s3:4566'),
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
    """Start HTTP server to serve S3 file API"""
    port = int(os.getenv('LAMBDA_PORT', 8080))
    print(f"S3 API server starting on port {port}...")
    
    server = HTTPServer(('0.0.0.0', port), LambdaHTTPHandler)
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down S3 API server...")
        server.shutdown()

if __name__ == "__main__":
    main()
