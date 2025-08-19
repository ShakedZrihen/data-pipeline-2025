from settings import PROVIDERS, s3, S3_BUCKET
from provider_handler import handle_provider

def main() -> None:
    # Initialize S3
    try:
        s3.create_bucket(Bucket=S3_BUCKET)
        print(f"Created bucket: {S3_BUCKET}")
    except s3.exceptions.BucketAlreadyExists:
        print(f"Bucket already exists: {S3_BUCKET}")
    except Exception as e:
        print(f"Error creating bucket: {e}")

    # Process providers
    for provider_name, config in PROVIDERS.items():
        print(f"\nStarting processing for {provider_name}")
        try:
            handle_provider(provider_name, config)
        except Exception as e:
            print(f"Failed to process {provider_name}: {e}")

if __name__ == "__main__":
    main()