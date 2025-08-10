import boto3

def delete_bucket(bucket_name="test-bucket"):
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    # First, empty the bucket by deleting all objects
    try:
        print(f"Emptying bucket '{bucket_name}'...")
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            if objects_to_delete:
                delete_response = s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': objects_to_delete}
                )
                deleted_count = len(delete_response.get('Deleted', []))
                print(f" Deleted {deleted_count} objects from bucket")
        else:
            print("Bucket is already empty")
            
    except Exception as e:
        print(f"Error emptying bucket: {e}")
        return
    
    # Now delete the bucket
    try:
        response = s3_client.delete_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' deleted successfully")
    except Exception as e:
        print(f"Error deleting bucket: {e}")
        return
    
    # Recreate the bucket
    try:
        response = s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' recreated successfully")
    except Exception as e:
        print(f"Error creating bucket: {e}")
        return

if __name__ == "__main__":
    delete_bucket()
