"""
Configuration file for the Extractor pipeline
"""

import os
from typing import Dict, Any

# Default configuration
DEFAULT_CONFIG = {
    'aws_region': 'us-east-1',
    'dynamodb_table': 'extractor-last-runs',
    'output_dir': '/tmp/extracted',
    'log_level': 'INFO'
}

def get_config() -> Dict[str, Any]:
    """Get configuration from environment variables with defaults"""
    config = DEFAULT_CONFIG.copy()
    
    # Override with environment variables
    config.update({
        'aws_region': os.environ.get('AWS_REGION', config['aws_region']),
        'dynamodb_table': os.environ.get('DYNAMODB_TABLE', config['dynamodb_table']),
        'output_dir': os.environ.get('OUTPUT_DIR', config['output_dir']),
        'log_level': os.environ.get('LOG_LEVEL', config['log_level']),
        'sqs_queue_url': os.environ.get('SQS_QUEUE_URL'),
        'test_connections': os.environ.get('TEST_CONNECTIONS', 'false').lower() == 'true'
    })
    
    return config

def validate_config(config: Dict[str, Any]) -> bool:
    """Validate configuration values"""
    required_fields = ['aws_region']
    
    for field in required_fields:
        if not config.get(field):
            print(f"Missing required configuration: {field}")
            return False
    
    return True
