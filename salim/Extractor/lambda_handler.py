from extractor import Extractor
from salim.utils.queue_handler import QueueHandler

def lambda_handler(event, context):
    queue_handler = QueueHandler()
    rabbitmq_available = queue_handler.setup_rabbitmq()
    extractor = Extractor()
    processed_files = []

    if rabbitmq_available:
        print("Sending files to RabbitMQ queue...")
        processed_files = extractor.create_individual_json_files(queue_handler.send_file_message)
        queue_handler.list_messages()
        queue_handler.close_connection()
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