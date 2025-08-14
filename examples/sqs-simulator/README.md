# SQS Simulator

A complete SQS (Amazon Simple Queue Service) simulator using LocalStack with a web interface to visualize and manage SQS messages in real-time.

## Architecture

- **LocalStack**: Simulates AWS SQS service locally
- **Lambda Function**: Python backend that handles SQS operations via HTTP API
- **React Frontend**: Web interface to view, send, and delete SQS messages
- **Test Script**: Python utility to send test messages to the queue

## Features

- 📨 **Real-time Message Viewing**: See SQS messages as they arrive
- ➕ **Send Messages**: Add new messages to the queue via web UI
- 🗑️ **Delete Messages**: Remove messages from the queue
- 📊 **Queue Statistics**: View visible and in-flight message counts
- 🔄 **Auto-refresh**: Automatically polls for new messages every 5 seconds
- 🧪 **Test Scripts**: Command-line utilities for testing

## Quick Start

1. **Start the services**:
   ```bash
   docker-compose up
   ```

2. **Open the web interface**:
   - Frontend: http://localhost:3001
   - Backend API: http://localhost:8081

3. **Send test messages**:
   ```bash
   # Send a single message
   python send_message_test.py send "Hello SQS!"
   
   # Send demo messages
   python send_message_test.py demo
   
   # Receive messages
   python send_message_test.py receive
   ```

## Services

### LocalStack (Port 4566)
- Simulates AWS SQS service
- Creates `test-queue` automatically on startup
- Stores data in Docker volume `localstack-data`

### Lambda Function (Port 8081)
API endpoints:
- `GET /messages` - List messages in queue
- `POST /send-message` - Send message to queue
- `POST /delete-message` - Delete message from queue

### Frontend (Port 3001)
React application with Material-UI components:
- Message list with real-time updates
- Send message dialog
- Delete message functionality
- Queue statistics display

## Test Script Usage

The `send_message_test.py` script provides several commands:

```bash
# Send a custom message
python send_message_test.py send "Your message here"

# Send JSON formatted message
python send_message_test.py send '{"event": "test", "data": "value"}'

# Receive all available messages
python send_message_test.py receive

# Send 5 demo messages for testing
python send_message_test.py demo
```

## Configuration

Environment variables (set in docker-compose.yml):

- `SQS_ENDPOINT`: LocalStack SQS endpoint (default: http://localstack:4566)
- `SQS_QUEUE_NAME`: Queue name (default: test-queue)
- `LAMBDA_PORT`: Lambda function port (default: 8081)
- `AWS_ACCESS_KEY_ID`: AWS credentials (default: test)
- `AWS_SECRET_ACCESS_KEY`: AWS credentials (default: test)
- `AWS_DEFAULT_REGION`: AWS region (default: us-east-1)

## Development

### Prerequisites
- Docker & Docker Compose
- Python 3.11+ (for test scripts)
- boto3 Python package: `pip install boto3`

### File Structure
```
sqs-simulator/
├── docker-compose.yml          # Docker services configuration
├── init-sqs.sh                 # SQS initialization script
├── send_message_test.py        # Test script for SQS operations
├── README.md                   # This file
├── lambda/
│   ├── Dockerfile              # Lambda function container
│   └── handler.py              # Lambda function code
└── frontend/
    ├── Dockerfile              # Frontend container
    ├── package.json            # React dependencies
    ├── public/
    │   └── index.html          # HTML template
    └── src/
        ├── index.js            # React entry point
        └── App.js              # Main React component
```

### Stopping the Services
```bash
docker-compose down
```

### Viewing Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f localstack
docker-compose logs -f lambda-function
docker-compose logs -f frontend
```

## Troubleshooting

1. **Port conflicts**: If ports 3001, 4566, or 8081 are in use, modify the ports in `docker-compose.yml`

2. **Messages not appearing**: Check that LocalStack is fully started before sending messages (wait ~10-15 seconds after `docker-compose up`)

3. **Frontend not loading**: Ensure all dependencies are installed with `docker-compose build --no-cache`

4. **Python script errors**: Install boto3: `pip install boto3`

## Comparison with S3 Simulator

Unlike the S3 simulator which shows uploaded files, this SQS simulator shows:
- Messages in queue instead of files
- Real-time message polling
- Message sending/deleting functionality
- Queue statistics (visible/in-flight messages)
- JSON message support for structured data

## Next Steps

- Add message filtering and search
- Implement Dead Letter Queue (DLQ) support
- Add message attributes display
- Implement batch operations
- Add message replay functionality