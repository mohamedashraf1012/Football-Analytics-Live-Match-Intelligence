import boto3
import ignore.config as config

s3 = boto3.client('s3',
                  aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)

response = s3.list_objects_v2(Bucket=config.S3_BUCKET, Prefix=config.S3_BRONZE_PREFIX)

if 'Contents' in response:
    # Filter: Exclude objects with 0 size (folders)
    actual_files = [obj for obj in response['Contents'] if obj['Size'] > 0]
    
    print(f"Total files found: {len(actual_files)}")
    print("-" * 60)
    
    # Display files with size in MB
    for i, obj in enumerate(actual_files, 1):
        file_name = obj['Key']
        file_size_mb = obj['Size'] / (1024 * 1024)
        print(f"{i}: {file_name} | Size: {file_size_mb:.2f} MB")
        
    print("-" * 60)
else:
    print("No files found.")