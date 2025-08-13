from extractor import Extractor
from queue_producer import QueueProducer

def lambda_handler(event, context):
    extractor = Extractor()
    processed_files = extractor.create_individual_json_files()
    queue_producer = QueueProducer()
    rabbitmq_available = queue_producer.setup_rabbitmq()
    
    if rabbitmq_available:
        print("Sending files to RabbitMQ queue...")
        for file_info in processed_files:
            queue_producer.send_file_message(
                file_info['json_file'],
                file_info['original_key'],
                file_info['size']
            )
        queue_producer.close_connection()
        print("All files sent to queue")
    else:
        print("RabbitMQ not available, skipping queue operations")
    
    response = {
        'statusCode': 200,
        'body': {
            'message': 'Files successfully converted to individual JSON format',
            'total_files': len(processed_files),
            'output_directory': extractor.output_dir,
            'summary_file': f"{extractor.output_dir}/extraction_summary.json",
            'rabbitmq_available': rabbitmq_available
        }
    }
    print(f"Lambda response: {response}")
    return response

def main():
    lambda_handler(None, None)

if __name__ == "__main__":
    main()