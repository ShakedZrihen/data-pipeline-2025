import boto3
import os

ENDPOINT_URL = "http://localhost:4566"
REGION_NAME = "us-east-1"
BUCKET_NAME = "providers"

def main():
    print("--- Uploading files to S3 ---")
    s3 = boto3.client('s3', endpoint_url=ENDPOINT_URL, region_name=REGION_NAME)
    
    providersName = ["keshet", "ramilevi", "yohananof"]
    folderPath = 'Crawler/out/'
    
    for provider in providersName:
        print(f"Running on Provider: {provider}")
        provider_full_path = os.path.join(folderPath, provider)
        if not os.path.exists(provider_full_path):
            print(f"  - Directory not found, skipping: {provider_full_path}")
            continue
            
        for branch_folder in os.listdir(provider_full_path):
            branch_path = os.path.join(provider_full_path, branch_folder)
            if os.path.isdir(branch_path):
                print(f"  - Running through Branch: {branch_folder}")
                for file_name in os.listdir(branch_path):
                    local_file_path = os.path.join(branch_path, file_name)
                    s3_key = f"{provider}/{branch_folder}/{file_name}"
                    with open(local_file_path, "rb") as f:
                        s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=f)
                    print(f"    ✅ Uploaded {file_name}")

    print("--- File upload complete ---\n")

if __name__ == "__main__":
    main()