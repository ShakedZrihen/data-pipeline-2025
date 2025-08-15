#!/usr/bin/env python3

import os
import sys
import logging
import argparse
from pathlib import Path
from typing import List, Dict, Any

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from file_processor import FileProcessor
from data_normalizer import DataNormalizer
from sqs_producer import SQSProducer
from db_handler import DynamoDBHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ExtractorOrchestrator:
    """Main orchestrator for the extractor pipeline"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Initialize components
        self.file_processor = FileProcessor()
        self.data_normalizer = DataNormalizer()
        
        # Initialize AWS services if configured
        self.sqs_producer = None
        self.db_handler = None
        
        if config.get('sqs_queue_url'):
            self.sqs_producer = SQSProducer(
                config['sqs_queue_url'], 
                config.get('aws_region', 'us-east-1')
            )
        
        if config.get('dynamodb_table'):
            self.db_handler = DynamoDBHandler(
                config['dynamodb_table'],
                config.get('aws_region', 'us-east-1')
            )
    
    def process_local_files(self, input_dir: str, output_dir: str) -> Dict[str, Any]:
        """Process local files and save results"""
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Find all .gz files
        gz_files = list(input_path.rglob("*.gz"))
        logger.info(f"Found {len(gz_files)} .gz files to process")
        
        results = {
            'processed': 0,
            'failed': 0,
            'files': []
        }
        
        for gz_file in gz_files:
            try:
                logger.info(f"Processing: {gz_file}")
                
                # Process the file
                metadata, content = self.file_processor.process_file(str(gz_file))
                
                # Normalize data
                normalized_data = self.data_normalizer.create_output_json(
                    metadata, content, gz_file.name
                )
                
                # Save JSON locally
                json_filename = f"{gz_file.stem}.json"
                json_path = output_path / json_filename
                self.data_normalizer.save_json_locally(normalized_data, str(json_path))
                
                # Send to SQS if configured
                if self.sqs_producer:
                    if self.sqs_producer.send_message(normalized_data):
                        logger.info(f"Sent to SQS: {gz_file.name}")
                        
                        # Update DynamoDB if configured
                        if self.db_handler:
                            provider = normalized_data['provider']
                            branch = normalized_data['branch']
                            file_type = normalized_data['type']
                            timestamp = normalized_data['timestamp']
                            
                            self.db_handler.update_last_run_time(
                                provider, branch, file_type, timestamp
                            )
                    else:
                        logger.error(f"Failed to send to SQS: {gz_file.name}")
                
                results['processed'] += 1
                results['files'].append({
                    'file': str(gz_file),
                    'status': 'success',
                    'json_file': str(json_path)
                })
                
            except Exception as e:
                logger.error(f"Failed to process {gz_file}: {e}")
                results['failed'] += 1
                results['files'].append({
                    'file': str(gz_file),
                    'status': 'failed',
                    'error': str(e)
                })
        
        return results
    
    def test_connections(self) -> Dict[str, bool]:
        """Test connections to AWS services"""
        results = {}
        
        if self.sqs_producer:
            results['sqs'] = self.sqs_producer.test_connection()
        else:
            results['sqs'] = False
        
        if self.db_handler:
            results['dynamodb'] = self.db_handler.test_connection()
            if results['dynamodb']:
                results['dynamodb_table'] = self.db_handler.create_table_if_not_exists()
        else:
            results['dynamodb'] = False
        
        return results

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Extractor Pipeline Orchestrator')
    parser.add_argument('--input-dir', required=True, help='Input directory containing .gz files')
    parser.add_argument('--output-dir', required=True, help='Output directory for JSON files')
    parser.add_argument('--sqs-queue-url', help='SQS queue URL for sending messages')
    parser.add_argument('--dynamodb-table', help='DynamoDB table name for tracking runs')
    parser.add_argument('--aws-region', default='us-east-1', help='AWS region')
    parser.add_argument('--test-connections', action='store_true', help='Test AWS connections')
    
    args = parser.parse_args()
    
    # Configuration
    config = {
        'sqs_queue_url': args.sqs_queue_url,
        'dynamodb_table': args.dynamodb_table,
        'aws_region': args.aws_region
    }
    
    # Initialize orchestrator
    orchestrator = ExtractorOrchestrator(config)
    
    # Test connections if requested
    if args.test_connections:
        logger.info("Testing AWS connections...")
        connection_results = orchestrator.test_connections()
        logger.info(f"Connection results: {connection_results}")
        
        if not any(connection_results.values()):
            logger.error("All connection tests failed. Check your AWS configuration.")
            return 1
    
    # Process files
    logger.info(f"Processing files from {args.input_dir} to {args.output_dir}")
    results = orchestrator.process_local_files(args.input_dir, args.output_dir)
    
    # Print results
    logger.info("=" * 50)
    logger.info("PROCESSING COMPLETE")
    logger.info("=" * 50)
    logger.info(f"Total files: {len(results['files'])}")
    logger.info(f"Successfully processed: {results['processed']}")
    logger.info(f"Failed: {results['failed']}")
    
    if results['failed'] > 0:
        logger.warning("Some files failed to process. Check the logs above.")
        return 1
    
    logger.info("All files processed successfully!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
