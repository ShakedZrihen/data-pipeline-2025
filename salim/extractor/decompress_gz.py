import boto3
import gzip
import shutil
import os

# S3 bucket name
bucket_name = "gov-price-files-hanif"

# Local save path (your extractor folder)
local_dir = r"C:\shenkar\Python Sedna\DATA-PIPELINE-2025-AMIR-KHALIFA-EXTRACTOR-submission\salim\extractor"

# Make sure local directory exists
os.makedirs(local_dir, exist_ok=True)

# Connect to S3
s3 = boto3.client("s3")

# List all objects in the bucket
response = s3.list_objects_v2(Bucket=bucket_name)

for obj in response.get("Contents", []):
    key = obj["Key"]

    if key.endswith(".gz"):
        # Build full local path with S3 folder structure
        local_gz = os.path.join(local_dir, key.replace("/", os.sep))
        local_out = local_gz.replace(".gz", "")

        # Make sure target directories exist
        os.makedirs(os.path.dirname(local_gz), exist_ok=True)

        # Download file
        print(f"Downloading {key} -> {local_gz}")
        s3.download_file(bucket_name, key, local_gz)

        # Decompress
        print(f"Decompressing {local_gz} -> {local_out}")
        with gzip.open(local_gz, "rb") as f_in:
            with open(local_out, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Delete .gz file
        os.remove(local_gz)
        print(f"Deleted compressed file: {local_gz}")

print("✅ All .gz files downloaded, decompressed, and organized into subfolders.")
