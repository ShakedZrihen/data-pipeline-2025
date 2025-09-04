import boto3
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, S3_BUCKET


class S3Manager:
    def __init__(self):
        self.s3_client = None
        self.bucket = S3_BUCKET
        self.setup_s3()
    
    def setup_s3(self):
        """Initialize S3 client with credentials from environment variables"""
        try:
            if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION]):
                print("ERROR: AWS credentials not found in environment variables")
                self.s3_client = None
                return
            
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_DEFAULT_REGION
            )
            print("S3 client initialized successfully")
        except Exception as e:
            print(f"Error setting up S3: {e}")
            self.s3_client = None
    
    def upload_to_s3(self, local_file_path, supermarket_name, branch_name, file_type, date):
        """Upload file to S3 with correct naming structure"""
        if not self.s3_client:
            print("S3 client not available, skipping upload")
            return False
        
        try:
            # Create S3 key with required structure
            # providers/<supermarket>/<branch>/<pricesFull_date.gz or promoFull_date.gz>
            if file_type.lower() == 'price':
                s3_filename = f"pricesFull_{date}.gz"
            else:  # promo
                s3_filename = f"promoFull_{date}.gz"
            
            s3_key = f"providers/{supermarket_name}/{branch_name}/{s3_filename}"
            
            # Upload to S3
            self.s3_client.upload_file(
                local_file_path,
                self.bucket,
                s3_key
            )
            
            print(f"[S3] Uploaded to s3://{self.bucket}/{s3_key}")
            return True
            
        except Exception as e:
            print(f"[S3] Upload failed: {e}")
            return False