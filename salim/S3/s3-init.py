import boto3
import os

def UploadFilesToS3(s3,bucketName):
    providersName = ["keshet","ramilevi","yohananof"]
    folderPath = '../Crawler/out/'

    for provider in providersName:
        print("Running on Provider:"+provider)
        for folderName in os.listdir(folderPath+provider):
            print("Running through SubProvider:"+folderName)
            try:
               currentFolderPath = folderPath+provider+"/"+folderName
               for file in os.listdir(currentFolderPath):
                 s3.upload_file(currentFolderPath+"/"+file, bucketName, provider+"/"+folderName+"/"+file)
                 print("✅ Uploaded to S3")

            except:
                print("seems like wrong")

def GetFilesFromBucket(s3,bucketName):
    objects = s3.list_objects_v2(Bucket=bucketName)
    print(f"\nObjects in bucket '{bucketName}")
    if 'Contents' in objects:
        for obj in objects['Contents']:
            print(" ", obj['Key'])
    else:
        print(" No objects found.")


def main():
    s3 = boto3.client(
    's3',
    endpoint_url="http://localhost:4566",  # LocalStack endpoint
    aws_access_key_id="test",              # dummy credentials
    aws_secret_access_key="test",
    region_name="us-east-1")

    bucket_name = "providers"
    s3.create_bucket(Bucket=bucket_name)

    UploadFilesToS3(s3,bucket_name)
    GetFilesFromBucket(s3,bucket_name)


if __name__ == "__main__":
    main()