from extractor import Extractor

def lambda_handler(event, context):
    extractor = Extractor()
    processed_files = extractor.create_individual_json_files()
    response = {
        'statusCode': 200,
        'body': {
            'message': 'Files successfully converted to individual JSON format',
            'total_files': len(processed_files),
            'output_directory': extractor.output_dir,
            'summary_file': f"{extractor.output_dir}/extraction_summary.json"
        }
    }
    print(f"Lambda response: {response}")
    return response

def main():
    lambda_handler(None, None)

if __name__ == "__main__":
    main()