# 🚀 Data Pipeline Extractor

A robust AWS Lambda-based extractor that processes compressed XML price and promotion files from S3, normalizes the data, and sends it to SQS queues while tracking processing history in DynamoDB.

## 📋 Overview

This extractor is designed to handle the data pipeline stage: **S3 → Extractor → SQS (JSON) → Lambda reads JSON and processes it**

The system automatically triggers when new `.gz` files are uploaded to S3, processes them according to the specified format, and generates normalized JSON output that matches the required schema.

## 🎯 Features

- **Automatic S3 Trigger**: Processes files as soon as they're uploaded
- **Multi-format Support**: Handles both old and new file naming conventions
- **Robust Error Handling**: Fault-tolerant processing with comprehensive logging
- **Data Normalization**: Converts XML data to standardized JSON format
- **SQS Integration**: Sends processed data to message queues
- **DynamoDB Tracking**: Maintains processing history and last run times
- **Local Testing**: Comprehensive test suite for development and validation

## 📁 File Format Support

### Old Format (Legacy)
```
Price7290055700007-0004-202508071000.gz
Promo7290055700007-1112-202508071100.gz
```

### New Format (Task Requirement)
```
providers/<provider>/<branch>/pricesFull_<timestamp>.gz
providers/<provider>/<branch>/promoFull_<timestamp>.gz
```

## 🏗️ Architecture

```
S3 Bucket → Lambda Function → File Processor → Data Normalizer → SQS Queue
                                    ↓
                              DynamoDB Table (Last Run Times)
```

### Components

1. **Lambda Function** (`lambda_function.py`): Main entry point and orchestration
2. **File Processor** (`file_processor.py`): Handles decompression and XML parsing
3. **Data Normalizer** (`data_normalizer.py`): Converts data to required JSON format
4. **SQS Producer** (`sqs_producer.py`): Sends messages to SQS queues
5. **DB Handler** (`db_handler.py`): Manages DynamoDB operations
6. **Configuration** (`config.py`): Environment-based configuration management

## 📊 Output JSON Schema

### Price Files
```json
{
  "provider": "yohananof",
  "branch": "תל אביב - יפו",
  "type": "pricesFull",
  "timestamp": "2025-08-06T18:00:00Z",
  "items": [
    {
      "product": "חלב תנובה 3%",
      "price": 5.9,
      "unit": "liter"
    }
  ]
}
```

### Promotion Files
```json
{
  "provider": "yohananof",
  "branch": "תל אביב - יפו",
  "type": "promoFull",
  "timestamp": "2025-08-06T18:00:00Z",
  "promotions": [
    {
      "promotion_id": "12345",
      "description": "חלב תנובה 3% - מבצע מיוחד",
      "start_date": "2025-08-06",
      "end_date": "2025-08-20",
      "discount_type": "percentage",
      "discount_value": "15"
    }
  ]
}
```

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- AWS CLI configured
- Required Python packages (see `requirements.txt`)

### Local Development

1. **Clone and Setup**
   ```bash
   git clone <repository-url>
   cd data-pipeline-2025
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r extractor/requirements.txt
   ```

2. **Run Tests**
   ```bash
   # Test existing functionality
   python test_extractor.py
   
   # Test new file format
   python test_new_format.py
   ```

3. **Check Output**
   - Processed files are saved to `extracted_output/` and `extracted_output_new_format/`
   - Review JSON files to verify data structure

### AWS Deployment

1. **Create Deployment Package**
   ```bash
   python deploy_extractor.py
   ```

2. **Deploy CloudFormation Stack**
   ```bash
   aws cloudformation deploy \
     --template-file extractor_cloudformation_final.yaml \
     --stack-name extractor-pipeline \
     --parameter-overrides BucketName=your-s3-bucket-name
   ```

3. **Configure S3 Trigger**
   - Upload the deployment ZIP to your S3 bucket
   - Configure S3 event notifications to trigger the Lambda function

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SQS_QUEUE_URL` | SQS queue URL for output messages | Required |
| `DYNAMODB_TABLE` | DynamoDB table name for tracking | `extractor-last-runs` |
| `AWS_REGION` | AWS region for services | `us-east-1` |
| `OUTPUT_DIR` | Local output directory | `/tmp/extracted` |
| `TEST_CONNECTIONS` | Test AWS connections on startup | `false` |

### AWS Services Required

- **S3**: Source file storage and Lambda trigger
- **Lambda**: Processing function execution
- **SQS**: Output message queue
- **DynamoDB**: Processing history tracking
- **IAM**: Lambda execution role with appropriate permissions

## 🧪 Testing

### Test Suite

The project includes comprehensive test suites:

- **`test_extractor.py`**: Tests existing file processing functionality
- **`test_new_format.py`**: Tests new file naming convention support

### Test Coverage

- ✅ File decompression and XML parsing
- ✅ Data extraction and normalization
- ✅ JSON output generation
- ✅ Error handling and fault tolerance
- ✅ New file format compatibility
- ✅ Batch processing capabilities

### Running Tests

```bash
# Run all tests
python test_extractor.py
python test_new_format.py

# Check output directories
ls extracted_output/
ls extracted_output_new_format/
```

## 🔧 Customization

### Adding New Providers

1. Update provider mapping in `data_normalizer.py`:
   ```python
   self.provider_mapping = {
       '7290055700007': 'carrefour',
       '7290058197699': 'goodpharm',
       # Add new provider here
       'NEW_CHAIN_ID': 'new_provider_name'
   }
   ```

2. Update branch mapping if needed:
   ```python
   self.branch_mapping = {
       '0004': 'תל אביב - יפו',
       # Add new branch mappings
       'NEW_STORE_ID': 'New Branch Name'
   }
   ```

### Modifying Output Schema

1. Update the `create_output_json` method in `data_normalizer.py`
2. Modify the normalization methods for items and promotions
3. Update tests to reflect new schema requirements

## 📈 Monitoring and Logging

### CloudWatch Logs

The Lambda function provides comprehensive logging:
- File processing status
- Data extraction details
- SQS message delivery status
- DynamoDB operation results
- Error details and stack traces

### DynamoDB Tracking

The system tracks:
- Last processing time per provider/branch/type combination
- Processing history and metadata
- Success/failure counts

### SQS Monitoring

Monitor queue metrics:
- Message delivery rates
- Queue depth and processing times
- Dead letter queue usage

## 🚨 Error Handling

### Fault Tolerance Features

- **File Validation**: Checks file format before processing
- **Graceful Degradation**: Continues processing other files if one fails
- **Comprehensive Logging**: Detailed error information for debugging
- **Fallback Mechanisms**: Default values for missing or invalid data

### Common Issues and Solutions

1. **Invalid File Format**
   - Check file naming convention
   - Verify file is properly compressed (.gz)
   - Ensure XML content is valid

2. **SQS Delivery Failures**
   - Check IAM permissions
   - Verify queue URL and region
   - Monitor queue capacity and throttling

3. **DynamoDB Errors**
   - Verify table exists and permissions
   - Check region configuration
   - Monitor table capacity and limits

## 🔒 Security

### IAM Permissions

The Lambda function requires:
- S3 read access to source bucket
- SQS send message permissions
- DynamoDB read/write access
- CloudWatch logging permissions

### Data Protection

- No sensitive data is logged
- Temporary files are cleaned up after processing
- Input validation prevents malicious file processing

## 📚 API Reference

### Lambda Function

**Handler**: `extractor.lambda_function.lambda_handler`

**Input Event**:
```json
{
  "Records": [
    {
      "s3": {
        "bucket": {"name": "bucket-name"},
        "object": {"key": "providers/provider/branch/filename.gz"}
      }
    }
  ]
}
```

**Output**:
```json
{
  "statusCode": 200,
  "body": {
    "status": "completed",
    "processed": 1,
    "failed": 0,
    "total": 1
  }
}
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is part of the Data Pipeline 2025 course requirements.

## 🆘 Support

For issues and questions:
1. Check the test output and logs
2. Review the configuration settings
3. Verify AWS service permissions
4. Check the troubleshooting section above

---


