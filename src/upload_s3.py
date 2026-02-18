import boto3
import os
from dotenv import load_dotenv

# 1. Load Secrets
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REGION = "eu-west-3" # Paris

# ⚠️ REPLACE THIS with your own unique bucket name!
BUCKET_NAME = "dsfs-1-enavarr-project-kayak" 

def upload_to_s3():
    # 2. Connect to S3
    print("🔌 Connecting to AWS S3...")
    s3 = boto3.client(
        "s3", 
        region_name=REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    
    # 3. Create Bucket (if it doesn't exist)
    try:
        s3.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={'LocationConstraint': REGION}
        )
        print(f"✅ Bucket '{BUCKET_NAME}' created (or already exists).")
    except Exception as e:
        if "BucketAlreadyOwnedByYou" in str(e):
            print(f"✅ Bucket '{BUCKET_NAME}' found.")
        else:
            print(f"⚠️ Warning during bucket creation: {e}")

    # 4. Upload File
    file_path = "data/processed/kayak_master.csv"
    object_name = "kayak_master.csv" # Name in S3
    
    if not os.path.exists(file_path):
        print(f"❌ Error: File {file_path} not found. Did you run process_data.py?")
        return

    print(f"⬆️ Uploading {object_name}...")
    try:
        s3.upload_file(file_path, BUCKET_NAME, object_name)
        print(f"🎉 Success! File uploaded to s3://{BUCKET_NAME}/{object_name}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")

if __name__ == "__main__":
    upload_to_s3()