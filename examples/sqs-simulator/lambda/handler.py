import boto3
import os
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from botocore.exceptions import ClientError

def lambda_handler(event, context=None):
    """AWS Lambda handler for SQS events"""
    print(f"Received event: {json.dumps(event, indent=2)}")
    
    try:
        if 'Records' in event:
            for record in event['Records']:
                message_body = record.get('body', '')
                receipt_handle = record.get('receiptHandle', '')
                message_id = record.get('messageId', '')
                
                print(f"🎯 SQS Message Received!")
                print(f"   Message ID: {message_id}")
                print(f"   Body: {message_body}")
                print(f"   Receipt Handle: {receipt_handle[:20]}...")
                print("-" * 50)
        else:
            print("No SQS records found in event")
            
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
        """Handle GET requests to list SQS messages"""
        try:
            if self.path == '/messages':
                sqs_client = boto3.client(
                    'sqs',
                    endpoint_url=os.getenv('SQS_ENDPOINT', 'http://localstack:4566'),
                    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
                    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
                    region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
                )
                
                queue_name = os.getenv('SQS_QUEUE_NAME', 'test-queue')
                
                try:
                    # Get queue URL
                    queue_url_response = sqs_client.get_queue_url(QueueName=queue_name)
                    queue_url = queue_url_response['QueueUrl']
                    
                    # Receive messages (up to 10)
                    response = sqs_client.receive_message(
                        QueueUrl=queue_url,
                        MaxNumberOfMessages=10,
                        WaitTimeSeconds=1,
                        MessageAttributeNames=['All']
                    )
                    
                    messages = []
                    if 'Messages' in response:
                        for msg in response['Messages']:
                            messages.append({
                                'messageId': msg['MessageId'],
                                'body': msg['Body'],
                                'receiptHandle': msg['ReceiptHandle'],
                                'md5OfBody': msg['MD5OfBody'],
                                'attributes': msg.get('Attributes', {}),
                                'messageAttributes': msg.get('MessageAttributes', {})
                            })
                    
                    # Get queue attributes
                    queue_attrs = sqs_client.get_queue_attributes(
                        QueueUrl=queue_url,
                        AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
                    )
                    
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        'messages': messages,
                        'queueName': queue_name,
                        'queueUrl': queue_url,
                        'approximateNumberOfMessages': queue_attrs['Attributes'].get('ApproximateNumberOfMessages', '0'),
                        'approximateNumberOfMessagesNotVisible': queue_attrs['Attributes'].get('ApproximateNumberOfMessagesNotVisible', '0')
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
                
                if self.path == '/send-message':
                    # Send message to SQS
                    data = json.loads(body)
                    message_body = data.get('message', '')
                    
                    sqs_client = boto3.client(
                        'sqs',
                        endpoint_url=os.getenv('SQS_ENDPOINT', 'http://localstack:4566'),
                        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
                        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
                        region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
                    )
                    
                    queue_name = os.getenv('SQS_QUEUE_NAME', 'test-queue')
                    queue_url_response = sqs_client.get_queue_url(QueueName=queue_name)
                    queue_url = queue_url_response['QueueUrl']
                    
                    response = sqs_client.send_message(
                        QueueUrl=queue_url,
                        MessageBody=message_body
                    )
                    
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        'messageId': response['MessageId'],
                        'md5OfBody': response['MD5OfBody']
                    }).encode('utf-8'))
                    
                elif self.path == '/delete-message':
                    # Delete message from SQS
                    data = json.loads(body)
                    receipt_handle = data.get('receiptHandle', '')
                    
                    sqs_client = boto3.client(
                        'sqs',
                        endpoint_url=os.getenv('SQS_ENDPOINT', 'http://localstack:4566'),
                        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
                        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
                        region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
                    )
                    
                    queue_name = os.getenv('SQS_QUEUE_NAME', 'test-queue')
                    queue_url_response = sqs_client.get_queue_url(QueueName=queue_name)
                    queue_url = queue_url_response['QueueUrl']
                    
                    sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps({'success': True}).encode('utf-8'))
                    
                else:
                    # Default lambda handler
                    event = json.loads(body)
                    response = lambda_handler(event)
                    
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(json.dumps(response).encode('utf-8'))
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
    port = int(os.getenv('LAMBDA_PORT', 8081))
    print(f"🚀 Lambda function server starting on port {port}...")
    
    server = HTTPServer(('0.0.0.0', port), LambdaHTTPHandler)
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down lambda function...")
        server.shutdown()

if __name__ == "__main__":
    main()