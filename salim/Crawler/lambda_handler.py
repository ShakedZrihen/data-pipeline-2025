import boto3
import base from base.py
import yohannof_crawler from yohannof_crawler.py as yohannof

def lambda_handler(event, context):
    driver=yohannof.get_driver()
    files_paths=yohannof.crawl(driver)
    for file_path in files_paths:
        yohannof.upload_file_to_s3(file_path, "yohannof")
    return {
        'statusCode': 200,
        'body': 'Crawler executed successfully'
    }

lambda_handler(None, None)