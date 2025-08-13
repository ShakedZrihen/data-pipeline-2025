# extractor/lambda_function.py
import boto3
import gzip
import json

# מייבאים את פונקציית הליבה מהקובץ השני
from file_parser import process_file_content 

# בעתיד נשתמש ב-boto3 כדי לתקשר עם S3, SQS ו-DynamoDB
# endpoint_url="http://localhost:4566" -> חיוני לעבודה עם LocalStack
s3_client = boto3.client('s3', endpoint_url="http://localhost:4566")

def lambda_handler(event, context):
    """
    זו הפונקציה הראשית ש-AWS Lambda תריץ.
    """
    # 1. קבלת פרטי הקובץ מהאירוע של S3
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    print(f"Lambda triggered for file: s3://{bucket_name}/{file_key}")

    try:
        # 2. קריאת הקובץ הדחוס מ-S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        gzipped_content = response['Body'].read()

        # 3. פריסת הדחיסה
        xml_content = gzip.decompress(gzipped_content).decode('utf-8')

        # 4. שימוש ב"מנוע" שלנו לעיבוד התוכן
        processed_json = process_file_content(file_key, xml_content)

        print("--- ✅ Successfully processed file. Result JSON: ---")
        print(json.dumps(processed_json, indent=2, ensure_ascii=False))

        # TODO (בשלבים הבאים):
        # 5. שליחת ה-JSON ל-SQS
        # 6. עדכון הזמן האחרון ב-DynamoDB

        return {
            'statusCode': 200,
            'body': json.dumps('File processed successfully!')
        }

    except Exception as e:
        print(f"!!! ERROR processing file {file_key}: {e}")
        raise e # זריקת השגיאה חשובה כדי ש-AWS ידע שהריצה נכשלה