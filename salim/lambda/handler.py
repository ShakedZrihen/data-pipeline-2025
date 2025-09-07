import os
import json
import boto3
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from botocore.exceptions import ClientError


S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://localstack:4566')
AWS_REGION  = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
S3_BUCKET   = os.getenv('S3_BUCKET', 'test-bucket')

s3 = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
    region_name=AWS_REGION,
)


def lambda_handler(event, context=None):
    print(f"Received event: {json.dumps(event, ensure_ascii=False)}")
    return {'statusCode': 200, 'body': json.dumps({'ok': True}, ensure_ascii=False)}


class LambdaHTTPHandler(BaseHTTPRequestHandler):
    def _json(self, code, payload):
        body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            if parsed.path == '/files':
                try:
                    resp = s3.list_objects_v2(Bucket=S3_BUCKET)
                    files = []
                    for obj in resp.get('Contents', []) or []:
                        files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'lastModified': obj['LastModified'].isoformat(),
                            'etag': obj['ETag'],
                        })
                    self._json(200, {'bucket': S3_BUCKET, 'files': files})
                except ClientError as e:
                    self._json(404, {'error': str(e)})
            else:
                self._json(404, {'error': 'Not found'})
        except Exception as e:
            self._json(500, {'error': str(e)})

    def do_POST(self):
        try:
            n = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(n).decode('utf-8') or '{}') if n > 0 else {}
            resp = lambda_handler(body)
            self._json(200, resp)
        except Exception as e:
            self._json(500, {'error': str(e)})

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def log_message(self, *a, **k):  # тишина в логах
        pass

def main():
    port = int(os.getenv('LAMBDA_PORT', 8080))
    print(f"🚀 Lambda HTTP server on {port}, bucket={S3_BUCKET}")
    HTTPServer(('0.0.0.0', port), LambdaHTTPHandler).serve_forever()

if __name__ == "__main__":
    main()