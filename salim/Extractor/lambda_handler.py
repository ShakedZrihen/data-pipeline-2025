from extractor import Extractor
from ..Crawler.delete_bucket import delete_bucket

def lambda_handler(event, context):
    extractor = Extractor()
    unified_data = extractor.create_unified_json()
    extractor.save_unified_json('unified_data.json')
    delete_bucket()
    response = {
        'statusCode': 200,
        'body': {
            'message': 'Files successfully converted to unified JSON format',
            'total_files': unified_data['total_files'],
            'file_types': unified_data['summary']['file_types'],
            'total_size': unified_data['summary']['total_size'],
            'output_file': 'unified_data.json'
        }
    }
    print(f"Lambda response: {response}")
    return response

def main():
    lambda_handler(None, None)

if __name__ == "__main__":
    main()