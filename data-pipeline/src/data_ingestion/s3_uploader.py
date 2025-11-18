import os
import boto3
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd

def upload_data_to_s3_versioned():
    """Upload data from data/raw/ to S3 bucket with versioning - SKIPS existing files"""
    
    print("Starting data upload to S3 with versioning...")
    
    # Load environment variables
    load_dotenv()
    
    # Get AWS credentials from .env file
    bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
    region = os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
    
    if not bucket_name:
        print("ERROR: AWS_S3_BUCKET_NAME not found in .env file!")
        return False
    
    print(f"Bucket: {bucket_name}")
    print(f"Region: {region}")
    
    try:
        # Create S3 client
        s3 = boto3.client('s3', region_name=region)
        
        # Create bucket if it doesn't exist
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f" Bucket '{bucket_name}' exists")
        except:
            print(f"Creating bucket '{bucket_name}' in region {region}...")
            if region == 'us-east-1':
                s3.create_bucket(Bucket=bucket_name)
            else:
                s3.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            print(f" Bucket created successfully!")

        # Find Parquet files in data/raw/
        data_folder = find_data_folder()
        if not data_folder:
            return False

        # Get list of Parquet files with validation
        parquet_files = validate_and_get_parquet_files(data_folder)
        if not parquet_files:
            return False
        
        print(f" Found {len(parquet_files)} valid Parquet files locally...")
        
        # Check which files already exist in S3
        existing_files = get_existing_s3_files(s3, bucket_name)
        files_to_upload = []
        
        for parquet_file in parquet_files:
            if parquet_file.name in existing_files:
                print(f"  Skipping {parquet_file.name} (already exists in S3)")
            else:
                files_to_upload.append(parquet_file)
        
        if not files_to_upload:
            print(" All files already exist in S3 - nothing to upload!")
            return True
        
        print(f" Uploading {len(files_to_upload)} new files...")
        
        # Generate upload session ID for versioning
        upload_session = datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f" Upload session: {upload_session}")

        # Upload only NEW files with versioning
        uploaded_count = 0
        for parquet_file in files_to_upload:
            success = upload_single_file(s3, bucket_name, parquet_file, upload_session)
            if success:
                uploaded_count += 1

        # Final summary
        if uploaded_count == len(files_to_upload):
            print(f"\n SUCCESS: All {uploaded_count} NEW Parquet files uploaded!")
            print(f" Upload session: {upload_session}")
            print(f" Next: Run processing pipeline to transform this data")
            return True
        else:
            print(f"\n Partial Success: {uploaded_count}/{len(files_to_upload)} new files uploaded")
            return False

    except Exception as e:
        print(f" ERROR: Upload failed: {e}")
        print("Check your AWS credentials and bucket permissions")
        return False

def get_existing_s3_files(s3, bucket_name):
    """Get list of all Parquet files already in S3 (from any upload session)"""
    existing_files = set()
    try:
        # List all objects in raw-data prefix
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix="raw-data/"):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.parquet'):
                        filename = obj['Key'].split('/')[-1]
                        existing_files.add(filename)
        
        print(f" Found {len(existing_files)} existing files in S3")
        return existing_files
        
    except Exception as e:
        print(f" Could not check existing S3 files: {e}")
        return set()

def find_data_folder():
    """Find the data/raw folder with multiple fallback paths"""
    possible_paths = [
        Path("data/raw"),
        Path("../data/raw"), 
        Path("../../data/raw"),
        Path("../../../data/raw")
    ]
    
    for path in possible_paths:
        if path.exists():
            print(f" Found data folder: {path}")
            return path
    
    print(" ERROR: Data folder not found in any location!")
    print("Searched in:", [str(p) for p in possible_paths])
    return None

def validate_and_get_parquet_files(data_folder):
    """Validate Parquet files before uploading"""
    parquet_files = list(data_folder.glob("*.parquet"))
    
    if not parquet_files:
        print(f" ERROR: No Parquet files found in {data_folder}")
        return []
    
    valid_files = []
    for parquet_file in parquet_files:
        # Simple file existence and size check only
        if parquet_file.exists() and parquet_file.stat().st_size > 0:
            file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
            print(f"   Valid: {parquet_file.name} ({file_size_mb:.1f} MB)")
            valid_files.append(parquet_file)
        else:
            print(f"   Invalid: {parquet_file.name} (file may be empty or corrupted)")
    
    if not valid_files:
        print(" ERROR: No valid Parquet files found!")
        return []
    
    return valid_files

def upload_single_file(s3, bucket_name, parquet_file, upload_session):
    """Upload a single file with proper error handling and latest link"""
    try:
        # Create versioned S3 key
        s3_key = f"raw-data/upload_{upload_session}/{parquet_file.name}"
        file_size_mb = parquet_file.stat().st_size / (1024 * 1024)

        print(f" Uploading {parquet_file.name} ({file_size_mb:.1f} MB)...")

        # Upload main file
        s3.upload_file(str(parquet_file), bucket_name, s3_key)
        print(f"   Uploaded: s3://{bucket_name}/{s3_key}")
        
        # Create latest symlink with better error handling
        latest_key = f"raw-data/latest/{parquet_file.name}"
        create_latest_symlink(s3, bucket_name, s3_key, latest_key, parquet_file.name)

        return True

    except Exception as upload_error:
        print(f"   ERROR: Failed to upload {parquet_file.name}: {upload_error}")
        return False

def create_latest_symlink(s3, bucket_name, source_key, latest_key, filename):
    """Create latest symlink with proper error handling"""
    try:
        # Ensure the latest folder exists by uploading an empty file if needed
        try:
            s3.head_object(Bucket=bucket_name, Key="raw-data/latest/")
        except:
            # Create the latest "folder" by uploading an empty object
            s3.put_object(Bucket=bucket_name, Key="raw-data/latest/", Body=b'')
        
        # Create the symlink via copy
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': source_key},
            Key=latest_key
        )
        print(f"      Latest: s3://{bucket_name}/{latest_key}")
    except Exception as copy_error:
        print(f"       Latest link failed (non-critical): {copy_error}")

def verify_upload_versioned():
    """Verify the uploaded Parquet files with versioning"""
    
    print("\n Verifying upload with versioning...")

    load_dotenv()
    bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
    region = os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')

    try:
        s3 = boto3.client('s3', region_name=region)

        # List all upload sessions
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix="raw-data/", Delimiter='/')
        
        print(" Upload sessions found:")
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                session_name = prefix['Prefix'].replace('raw-data/', '').replace('/', '')
                if session_name != 'latest':  # Don't show latest as a session
                    print(f"   {session_name}")

        # Show latest files
        latest_response = s3.list_objects_v2(Bucket=bucket_name, Prefix="raw-data/latest/")
        
        if "Contents" not in latest_response:
            print(" No latest files found!")
            return False

        files = latest_response["Contents"]
        # Filter out the folder marker object
        files = [f for f in files if f['Key'] != 'raw-data/latest/']
        
        print(f"\n🔗 Latest files ({len(files)}):")
        
        total_size_mb = 0
        for file in files:
            size_mb = file['Size'] / (1024 * 1024)
            total_size_mb += size_mb
            filename = file['Key'].split('/')[-1]
            print(f"   {filename} ({size_mb:.2f} MB)")

        print(f"\n Latest Upload Summary:")
        print(f"  Total files: {len(files)}")
        print(f"  Total data size: {total_size_mb:.2f} MB")
        print(f"  Last modified: {files[0]['LastModified'] if files else 'N/A'}")
        
        return True
    
    except Exception as e:
        print(f" Verification Failed: {e}")
        return False

def check_parquet_files():
    """Check if Parquet files exist locally before uploading"""
    
    print(" Checking for local Parquet files...")
    
    data_folder = find_data_folder()
    if not data_folder:
        return False
    
    parquet_files = list(data_folder.glob("*.parquet"))
    csv_files = list(data_folder.glob("*.csv"))
    
    print(f" Found {len(parquet_files)} Parquet files:")
    for pf in parquet_files:
        size_mb = pf.stat().st_size / (1024 * 1024)
        print(f"   {pf.name} ({size_mb:.2f} MB)")
    
    if csv_files:
        print(f"\n Found {len(csv_files)} CSV files (will be ignored):")
        for cf in csv_files:
            print(f"  ❌ {cf.name}")
        print("Note: Only Parquet files will be uploaded")
    
    return len(parquet_files) > 0

if __name__ == "__main__":
    # First check if we have Parquet files
    has_parquet_files = check_parquet_files()
    
    if not has_parquet_files:
        print("\n No Parquet files found to upload!")
        print("Please make sure you have Parquet files in data/raw/ folder")
        exit(1)
    
    # Run versioned upload
    success = upload_data_to_s3_versioned()
    
    # Verify if upload was successful
    if success:
        verify_upload_versioned()
    
    print("\n Next step: Run the processing pipeline to transform this data!")
    print("   python our_processing_script.py")