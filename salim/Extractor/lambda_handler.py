from extractor import Extractor
from queue_producer import publish_from_files
import os

def lambda_handler(event, context):
    extractor = Extractor()
    processed_files = extractor.create_individual_json_files()
    
    # Get the output directory path
    output_dir = extractor.output_dir
    
    try:
        print("Sending files to RabbitMQ queue...")
        # Use the new publish_from_files function
        publish_from_files([output_dir])
        print("All files sent to queue")
        rabbitmq_available = True
    except Exception as e:      
        print(f"RabbitMQ error: {e}")
        rabbitmq_available = False
    
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