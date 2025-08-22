import boto3

# LocalStack S3 client
s3 = boto3.resource(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",      # dummy creds for LocalStack
    aws_secret_access_key="test",
    region_name="us-east-1"
)

bucket_name = "providers-bucket"
bucket = s3.Bucket(bucket_name)

# Delete all objects
print(f"Deleting all objects from {bucket_name}...")
bucket.objects.all().delete()

# Delete all versions (if bucket has versioning enabled)
try:
    bucket.object_versions.all().delete()
except Exception as e:
    print(f"No versioned objects or error deleting versions: {e}")

print(f"All contents of bucket '{bucket_name}' deleted.")
