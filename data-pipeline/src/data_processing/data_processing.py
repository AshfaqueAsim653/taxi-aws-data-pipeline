import pandas as pd
import boto3
import os
import tempfile
from datetime import datetime, timezone
import numpy as np
import json
from dotenv import load_dotenv
import re

# Watermark storage in S3
WATERMARK_KEY = "processed/_metadata/last_processed_watermark.txt"
PROCESSED_TRACKER_KEY = "processed/_metadata/processed_files.json"

def extract_year_month_from_dataset_name(dataset_name):
    """Extract year and month from dataset filename for validation"""
    # Pattern to match yellow_tripdata_YYYY-MM.parquet
    pattern = r'yellow_tripdata_(\d{4})-(\d{2})\.parquet'
    match = re.search(pattern, dataset_name)
    
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        return year, month
    return None

def validate_data_against_filename(df, dataset_name):
    """Validate that data matches the expected year/month from filename"""
    expected_year_month = extract_year_month_from_dataset_name(dataset_name)
    
    if not expected_year_month:
        print(f"    Could not extract year/month from filename: {dataset_name}")
        return df
    
    expected_year, expected_month = expected_year_month
    print(f"   Validating data against expected: {expected_year}-{expected_month:02d}")
    
    # Check if we have the required datetime columns
    if 'tpep_pickup_datetime' not in df.columns:
        print(f"    Missing datetime columns for validation")
        return df
    
    # Extract year and month from the actual data
    df['data_pickup_year'] = df['tpep_pickup_datetime'].dt.year
    df['data_pickup_month'] = df['tpep_pickup_datetime'].dt.month
    
    # Count records that don't match the expected year/month
    mismatched_mask = (df['data_pickup_year'] != expected_year) | (df['data_pickup_month'] != expected_month)
    mismatched_count = mismatched_mask.sum()
    
    if mismatched_count > 0:
        # Show what unexpected years/months were found
        mismatched_data = df[mismatched_mask]
        unexpected_years = sorted(mismatched_data['data_pickup_year'].unique())
        unexpected_months = sorted(mismatched_data['data_pickup_month'].unique())
        
        print(f"   Found {mismatched_count:,} mismatched records")
        print(f"   Unexpected years: {unexpected_years}")
        print(f"   Unexpected months: {unexpected_months}")
        
        # Filter out mismatched records
        df = df[~mismatched_mask]
        print(f"   Filtered out {mismatched_count:,} mismatched records")
        print(f"   Remaining records: {len(df):,}")
    else:
        print(f"   All records match expected year/month")
    
    # Clean up temporary columns
    df = df.drop(['data_pickup_year', 'data_pickup_month'], axis=1, errors='ignore')
    
    return df

def clean_taxi_data(df):
    """Comprehensive data cleaning for taxi data"""
    
    print(" Cleaning data...")
    original_rows = len(df)
    
    # Create a copy to avoid modifying the original
    df_cleaned = df.copy()
    
    # 1. Handle missing values
    print("   Handling missing values...")
    
    # Numeric columns - fill with median
    numeric_columns = ['passenger_count', 'trip_distance', 'fare_amount', 'extra', 
                      'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
                      'total_amount', 'congestion_surcharge', 'airport_fee']
    
    for col in numeric_columns:
        if col in df_cleaned.columns:
            missing_before = df_cleaned[col].isna().sum()
            if missing_before > 0:
                df_cleaned[col] = df_cleaned[col].fillna(df_cleaned[col].median())
                print(f"     Filled {missing_before:,} missing values in {col}")
    
    # Categorical columns - fill with mode
    categorical_columns = ['store_and_fwd_flag', 'RatecodeID', 'payment_type']
    for col in categorical_columns:
        if col in df_cleaned.columns and df_cleaned[col].notna().any():
            missing_before = df_cleaned[col].isna().sum()
            if missing_before > 0:
                mode_val = df_cleaned[col].mode().iloc[0] if not df_cleaned[col].mode().empty else 'Unknown'
                df_cleaned[col] = df_cleaned[col].fillna(mode_val)
                print(f"     Filled {missing_before:,} missing values in {col}")
    
    # Location IDs - fill with 0 (unknown location)
    location_columns = ['PULocationID', 'DOLocationID']
    for col in location_columns:
        if col in df_cleaned.columns:
            missing_before = df_cleaned[col].isna().sum()
            if missing_before > 0:
                df_cleaned[col] = df_cleaned[col].fillna(0)
                print(f"     Filled {missing_before:,} missing values in {col}")
    
    # 2. Remove duplicate records
    print("   Removing duplicate records...")
    duplicates_before = df_cleaned.duplicated().sum()
    if duplicates_before > 0:
        df_cleaned = df_cleaned.drop_duplicates()
        print(f"     Removed {duplicates_before:,} duplicate records")
    
    # 3. Handle outliers and invalid values
    print("   Handling outliers and invalid values...")
    
    # Trip distance - reasonable bounds (0 to 100 miles)
    if 'trip_distance' in df_cleaned.columns:
        outlier_mask = (df_cleaned['trip_distance'] <= 0) | (df_cleaned['trip_distance'] > 100)
        outliers_before = outlier_mask.sum()
        if outliers_before > 0:
            # Cap extreme values at 99th percentile
            p99 = df_cleaned['trip_distance'].quantile(0.99)
            df_cleaned.loc[outlier_mask, 'trip_distance'] = p99
            print(f"     Capped {outliers_before:,} outlier trip distances")
    
    # Fare amount - reasonable bounds (0 to $500)
    if 'fare_amount' in df_cleaned.columns:
        outlier_mask = (df_cleaned['fare_amount'] < 0) | (df_cleaned['fare_amount'] > 500)
        outliers_before = outlier_mask.sum()
        if outliers_before > 0:
            # Cap extreme values at 99th percentile
            p99 = df_cleaned['fare_amount'][df_cleaned['fare_amount'] >= 0].quantile(0.99)
            df_cleaned.loc[outlier_mask, 'fare_amount'] = p99
            print(f"     Capped {outliers_before:,} outlier fare amounts")
    
    # Passenger count - reasonable bounds (1 to 6)
    if 'passenger_count' in df_cleaned.columns:
        outlier_mask = (df_cleaned['passenger_count'] < 1) | (df_cleaned['passenger_count'] > 6)
        outliers_before = outlier_mask.sum()
        if outliers_before > 0:
            # Cap at reasonable values
            df_cleaned.loc[df_cleaned['passenger_count'] < 1, 'passenger_count'] = 1
            df_cleaned.loc[df_cleaned['passenger_count'] > 6, 'passenger_count'] = 6
            print(f"     Fixed {outliers_before:,} outlier passenger counts")
    
    # 4. Data consistency fixes
    print("   Applying data consistency fixes...")
    
    # Ensure total_amount is consistent with component sum
    if all(col in df_cleaned.columns for col in ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 
                                               'tolls_amount', 'improvement_surcharge', 
                                               'congestion_surcharge', 'airport_fee', 'total_amount']):
        component_sum = (df_cleaned['fare_amount'] + df_cleaned['extra'] + df_cleaned['mta_tax'] + 
                        df_cleaned['tip_amount'] + df_cleaned['tolls_amount'] + 
                        df_cleaned['improvement_surcharge'] + df_cleaned['congestion_surcharge'] + 
                        df_cleaned['airport_fee'])
        
        # Flag records where total_amount doesn't match component sum (within tolerance)
        amount_discrepancy = abs(df_cleaned['total_amount'] - component_sum) > 0.1
        discrepancies = amount_discrepancy.sum()
        if discrepancies > 0:
            # Use component sum where discrepancy is large
            df_cleaned.loc[amount_discrepancy, 'total_amount'] = component_sum
            print(f"     Fixed {discrepancies:,} total amount discrepancies")
    
    # Fix datetime inconsistencies (dropoff before pickup)
    if all(col in df_cleaned.columns for col in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']):
        time_inconsistencies = (df_cleaned['tpep_dropoff_datetime'] < df_cleaned['tpep_pickup_datetime']).sum()
        if time_inconsistencies > 0:
            # For inconsistent times, set dropoff to 5 minutes after pickup
            mask = df_cleaned['tpep_dropoff_datetime'] < df_cleaned['tpep_pickup_datetime']
            df_cleaned.loc[mask, 'tpep_dropoff_datetime'] = (
                df_cleaned.loc[mask, 'tpep_pickup_datetime'] + pd.Timedelta(minutes=5)
            )
            print(f"     Fixed {time_inconsistencies:,} datetime inconsistencies")
    
    # 5. Remove clearly invalid records that can't be fixed
    print("  🔍 Removing invalid records...")
    
    invalid_mask = (
        (df_cleaned['fare_amount'] <= 0) |
        (df_cleaned['trip_distance'] <= 0) |
        (df_cleaned['tpep_pickup_datetime'].isna()) |
        (df_cleaned['tpep_dropoff_datetime'].isna())
    )
    
    invalid_count = invalid_mask.sum()
    if invalid_count > 0:
        df_cleaned = df_cleaned[~invalid_mask]
        print(f"     Removed {invalid_count:,} fundamentally invalid records")
    
    # Final summary
    cleaned_rows = len(df_cleaned)
    rows_removed = original_rows - cleaned_rows
    cleaning_rate = (rows_removed / original_rows) * 100
    
    print(f"   Cleaning complete: {original_rows:,} → {cleaned_rows:,} rows")
    print(f"   Removed {rows_removed:,} rows ({cleaning_rate:.2f}%)")
    
    return df_cleaned

def deduplicate_files(file_objects):
    """Remove duplicate files, keeping only the most recent version of each file"""
    unique_files = {}
    
    for file_obj in file_objects:
        filename = os.path.basename(file_obj['Key'])
        
        # If we haven't seen this filename, or this one is newer, keep it
        if filename not in unique_files or file_obj['LastModified'] > unique_files[filename]['LastModified']:
            unique_files[filename] = file_obj
    
    # Return only the most recent version of each file
    return list(unique_files.values())

def get_processing_watermark(s3, bucket_name):
    """Get the last processed timestamp watermark"""
    try:
        response = s3.get_object(Bucket=bucket_name, Key=WATERMARK_KEY)
        watermark_str = response['Body'].read().decode('utf-8').strip()
        return datetime.fromisoformat(watermark_str)
    except s3.exceptions.NoSuchKey:
        # First run - start from beginning of time
        return datetime(2000, 1, 1, tzinfo=timezone.utc)
    except Exception as e:
        print(f"Warning: Could not read watermark, using default: {e}")
        return datetime(2000, 1, 1, tzinfo=timezone.utc)

def update_processing_watermark(s3, bucket_name, new_watermark):
    """Update the watermark to track progress"""
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=WATERMARK_KEY,
            Body=new_watermark.isoformat().encode('utf-8')
        )
        print(f" Updated watermark: {new_watermark}")
    except Exception as e:
        print(f"Warning: Could not update watermark: {e}")

def get_processed_files_tracker(s3, bucket_name):
    """Get the set of already processed files"""
    try:
        response = s3.get_object(Bucket=bucket_name, Key=PROCESSED_TRACKER_KEY)
        processed_data = json.loads(response['Body'].read().decode('utf-8'))
        return set(processed_data.get('processed_files', []))
    except s3.exceptions.NoSuchKey:
        return set()
    except Exception as e:
        print(f"Warning: Could not read processed files tracker: {e}")
        return set()

def mark_files_processed(s3, bucket_name, file_keys):
    """Mark files as processed in the tracker"""
    try:
        processed_files = get_processed_files_tracker(s3, bucket_name)
        processed_files.update(file_keys)
        
        # Keep only last 1000 files to avoid infinite growth
        if len(processed_files) > 1000:
            processed_files = set(list(processed_files)[-1000:])
        
        s3.put_object(
            Bucket=bucket_name,
            Key=PROCESSED_TRACKER_KEY,
            Body=json.dumps({'processed_files': list(processed_files)})
        )
        print(f" Marked {len(file_keys)} files as processed")
    except Exception as e:
        print(f"Warning: Could not update processed files tracker: {e}")

def find_files_since_watermark(s3, bucket_name, watermark):
    """Find all files modified since the watermark"""
    try:
        paginator = s3.get_paginator('list_objects_v2')
        all_objects = []
        
        for page in paginator.paginate(Bucket=bucket_name, Prefix="raw-data/"):
            if 'Contents' in page:
                all_objects.extend(page['Contents'])
        
        # Filter for yellow taxi Parquet files modified since watermark
        new_files = [
            obj for obj in all_objects 
            if ('yellow_tripdata_' in obj['Key'] and 
                obj['Key'].endswith('.parquet') and
                obj['LastModified'] > watermark)
        ]
        
        # Sort by modification time (oldest first for orderly processing)
        new_files.sort(key=lambda x: x['LastModified'])
        
        return new_files
        
    except Exception as e:
        print(f"Error finding files: {e}")
        return []

def process_incremental_files():
    """Process only new files using watermark + idempotent pattern"""
    
    print(" Starting incremental file processing with Watermark + Idempotent Combo...")
    
    # Load environment variables
    load_dotenv()
    bucket_name = os.getenv('AWS_S3_BUCKET_NAME')
    region = os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
    
    try:
        s3 = boto3.client('s3', region_name=region)
        
        # Step 1: Get watermark and processed files tracker
        watermark = get_processing_watermark(s3, bucket_name)
        processed_files = get_processed_files_tracker(s3, bucket_name)
        
        print(f" Last processed watermark: {watermark}")
        print(f" Already processed files: {len(processed_files)}")
        
        # Step 2: Find new files since watermark
        new_files = find_files_since_watermark(s3, bucket_name, watermark)
        print(f" Found {len(new_files)} new file(s) since watermark")
        
        if not new_files:
            print(" No new files to process")
            return True
        
        # Step 2.5: DEDUPLICATE - Remove duplicate files, keep only most recent
        new_files = deduplicate_files(new_files)
        print(f" After deduplication: {len(new_files)} unique files")
        
        # Step 3: Filter out already processed files (idempotency check)
        files_to_process = [
            file_obj for file_obj in new_files 
            if file_obj['Key'] not in processed_files
        ]
        
        if not files_to_process:
            print(" All new files have already been processed")
            # Still update watermark to current time to avoid checking old files
            update_processing_watermark(s3, bucket_name, datetime.now(timezone.utc))
            return True
        
        print(f" Processing {len(files_to_process)} new, unprocessed files:")
        for file_obj in files_to_process:
            filename = os.path.basename(file_obj['Key'])
            print(f"  - {filename} (modified: {file_obj['LastModified']})")
        
        # Step 4: Process the files (your existing logic)
        file_keys = [file_obj['Key'] for file_obj in files_to_process]
        processing_success = process_files_with_existing_logic(s3, bucket_name, file_keys)
        
        # Step 5: Update tracking if successful
        if processing_success:
            # Mark files as processed
            mark_files_processed(s3, bucket_name, file_keys)
            
            # Update watermark to the latest processed file timestamp
            latest_timestamp = max(obj['LastModified'] for obj in files_to_process)
            update_processing_watermark(s3, bucket_name, latest_timestamp)
            
            print(f" Successfully processed {len(files_to_process)} new files")
            print(f" Updated watermark to: {latest_timestamp}")
        else:
            print(" Processing failed - no files marked as processed")
        
        return processing_success
        
    except Exception as e:
        print(f" Incremental processing failed: {e}")
        return False

def process_files_with_existing_logic(s3, bucket_name, parquet_files):
    """Your existing processing logic adapted for incremental processing"""
    
    print("\n" + "="*60)
    print("Processing files with existing logic...")
    
    if len(parquet_files) < 1:
        print("No files found to process!")
        return False
    
    print(f"Processing {len(parquet_files)} files: {[os.path.basename(f) for f in parquet_files]}")
    
    # Step 1: Load files with memory optimization
    print("\nStep 1: Loading Parquet files...")
    datasets = load_files_with_memory_optimization(s3, bucket_name, parquet_files)
    
    if not datasets:
        print(" Failed to load any files")
        return False
    
    # Step 2: Union with schema handling
    print("\nStep 2: Combining datasets...")
    combined_data = efficient_union_dataframes(datasets)
    
    print(f"Combined data: {len(combined_data):,} rows, {len(combined_data.columns)} columns")
    
    # Step 3: Apply consistent schema
    print("\nStep 3: Applying consistent data types...")
    processed_data = apply_optimized_schema(combined_data)
    
    # NEW STEP: Data Cleaning
    print("\nStep 3.5: Cleaning data...")
    cleaned_data = clean_taxi_data(processed_data)
    
    # Step 4: Your existing transformations
    print("\nStep 4: Transforming data...")
    transformed_data = transform_taxi_data({'combined': cleaned_data})
    
    # Step 5: Create metrics and upload
    print("\nStep 5: Creating business metrics...")
    business_metrics = create_taxi_metrics(transformed_data)
    
    # Step 6: Upload processed data with versioning
    print("\nStep 6: Uploading processed data...")
    
    # Generate processing info from filenames (FAST)
    processing_id, date_range = generate_processing_info_from_filenames(parquet_files)
    upload_success = upload_processed_taxi_data_versioned(
        s3, bucket_name, transformed_data, business_metrics, processing_id, date_range
    )
    
    return upload_success

def generate_processing_info_from_filenames(parquet_files):
    """Extract date range from S3 filenames for versioning"""
    
    # Extract years and months from filenames
    months = []
    for s3_key in parquet_files:
        filename = os.path.basename(s3_key)
        # yellow_tripdata_2024-01.parquet → "2024-01"
        if 'yellow_tripdata_' in filename:
            date_part = filename.replace('yellow_tripdata_', '').replace('.parquet', '')
            months.append(date_part)
    
    if months:
        # Sort to get range
        months.sort()
        date_range = f"{months[0]}_{months[-1]}"
    else:
        # Fallback to current date
        current_date = datetime.now().strftime("%Y-%m")
        date_range = f"{current_date}_{current_date}"
    
    # Generate unique processing ID with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    processing_id = f"proc_{timestamp}"
    
    print(f" Date range from filenames: {date_range}")
    print(f" Processing ID: {processing_id}")
    
    return processing_id, date_range

def upload_processed_taxi_data_versioned(s3, bucket_name, processed_datasets, business_metrics, processing_id, date_range):
    """Upload processed taxi data and metrics to S3 with versioning"""
    
    upload_count = 0
    
    # Upload processed datasets with versioning
    for dataset_name, df in processed_datasets.items():
        try:
            # Save as parquet file
            local_path = os.path.join(tempfile.gettempdir(), f"{dataset_name}.parquet")
            df.to_parquet(local_path, index=False, compression='snappy')
            
            # Upload to S3 with versioning
            s3_key = f"processed/taxi/{processing_id}/{dataset_name}.parquet"
            s3.upload_file(local_path, bucket_name, s3_key)
            
            # Also create a latest symlink for easy access
            latest_key = f"processed/taxi/latest/{dataset_name}.parquet"
            try:
                s3.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': s3_key},
                    Key=latest_key
                )
                print(f" Uploaded {dataset_name}: {len(df):,} records")
                print(f"   Versioned: {s3_key}")
                print(f"   Latest: {latest_key}")
            except Exception as copy_error:
                print(f" Uploaded {dataset_name}: {len(df):,} records")
                print(f"   Versioned: {s3_key}")
                print(f"    Latest link failed: {copy_error}")
            
            upload_count += 1
            
            # Clean up
            os.remove(local_path)
            
        except Exception as e:
            print(f" Failed to upload {dataset_name}: {e}")
    
    # Upload business metrics with date-based partitioning
    for metric_name, df in business_metrics.items():
        try:
            # Save as CSV for metrics
            local_path = os.path.join(tempfile.gettempdir(), f"{metric_name}.csv")
            df.to_csv(local_path, index=False)
            
            # Upload to S3 with date versioning
            s3_key = f"processed/metrics/date={date_range}/{processing_id}_{metric_name}.csv"
            s3.upload_file(local_path, bucket_name, s3_key)
            
            # Also create latest version
            latest_key = f"processed/metrics/latest/{metric_name}.csv"
            try:
                s3.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': s3_key},
                    Key=latest_key
                )
                print(f" Uploaded {metric_name}: {len(df)} records")
                print(f"   Versioned: {s3_key}")
                print(f"   Latest: {latest_key}")
            except Exception as copy_error:
                print(f" Uploaded {metric_name}: {len(df)} records")
                print(f"   Versioned: {s3_key}")
                print(f"    Latest link failed: {copy_error}")
            
            upload_count += 1
            
            # Clean up
            os.remove(local_path)
            
        except Exception as e:
            print(f" Failed to upload {metric_name}: {e}")
    
    total_files = len(processed_datasets) + len(business_metrics)
    success = upload_count == total_files
    
    if success:
        print(f" Successfully uploaded all {upload_count} files to S3!")
        print(f" Processing ID: {processing_id}")
        print(f" Date range: {date_range}")
    else:
        print(f" Partial upload: {upload_count}/{total_files} files")
    
    return success

def get_files_to_process(s3, bucket_name, max_files=2):
    """Get only the files that need processing (searching recursively through subfolders)"""
    
    try:
        # List ALL objects recursively under raw-data/ using paginator
        paginator = s3.get_paginator('list_objects_v2')
        all_objects = []
        
        print("Searching for Parquet files in raw-data/ and subfolders...")
        for page in paginator.paginate(Bucket=bucket_name, Prefix="raw-data/"):
            if 'Contents' in page:
                all_objects.extend(page['Contents'])
        
        if not all_objects:
            print("No objects found in raw-data/")
            return []
        
        # Filter for yellow taxi Parquet files (now searches subfolders too)
        parquet_files = [
            obj for obj in all_objects 
            if 'yellow_tripdata_' in obj['Key'] and obj['Key'].endswith('.parquet')
        ]
        
        print(f"Found {len(parquet_files)} Parquet files total")
        
        # Sort by last modified (newest first) and take max_files
        parquet_files.sort(key=lambda x: x['LastModified'], reverse=True)
        
        selected_files = [obj['Key'] for obj in parquet_files[:max_files]]
        
        print(f"Selected {len(selected_files)} most recent files:")
        for file in selected_files:
            print(f"  - {file}")
        
        return selected_files
        
    except Exception as e:
        print(f"Error listing files: {e}")
        return []

def load_files_with_memory_optimization(s3, bucket_name, s3_keys):
    """Load files with memory-efficient settings"""
    
    datasets = {}
    
    for s3_key in s3_keys:
        try:
            file_name = os.path.basename(s3_key)
            print(f"Loading {file_name}...")
            
            # Download to temporary file
            local_path = os.path.join(tempfile.gettempdir(), file_name)
            s3.download_file(bucket_name, s3_key, local_path)
            
            # Load with optimized settings for large files
            df = pd.read_parquet(
                local_path,
                engine='pyarrow',  # Faster than fastparquet for large files
                use_pandas_metadata=True
            )
            
            # NEW: Validate data against filename before optimization
            df = validate_data_against_filename(df, file_name)
            
            # Optimize memory usage
            df = optimize_dataframe_memory(df)
            
            datasets[file_name] = df
            print(f"   Loaded: {len(df):,} rows, {len(df.columns)} columns")
            print(f"   Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
            
            # Clean up
            os.remove(local_path)
            
        except Exception as e:
            print(f"   Failed to load {file_name}: {e}")
    
    return datasets

def optimize_dataframe_memory(df):
    """Optimize DataFrame memory usage"""
    
    # Convert object columns to category if they have low cardinality
    for col in df.select_dtypes(include=['object']):
        if df[col].nunique() / len(df) < 0.5:  # If < 50% unique values
            df[col] = df[col].astype('category')
    
    # Downcast numeric columns where possible
    for col in df.select_dtypes(include=['int64']):
        df[col] = pd.to_numeric(df[col], downcast='integer')
    
    for col in df.select_dtypes(include=['float64']):
        df[col] = pd.to_numeric(df[col], downcast='float')
    
    return df

def efficient_union_dataframes(datasets):
    """Efficiently combine DataFrames with schema handling"""
    
    if not datasets:
        return pd.DataFrame()
    
    dataframes = list(datasets.values())
    
    # Get all unique columns across all DataFrames
    all_columns = set()
    for df in dataframes:
        all_columns.update(df.columns)
    
    print(f"Columns across all files: {list(all_columns)}")
    
    # Align all DataFrames to have the same columns
    aligned_dfs = []
    for name, df in datasets.items():
        aligned_df = df.copy()
        
        # Add missing columns with null values
        for col in all_columns:
            if col not in aligned_df.columns:
                aligned_df[col] = None
        
        # Reorder columns consistently
        aligned_df = aligned_df[list(all_columns)]
        aligned_dfs.append(aligned_df)
    
    # Concatenate all aligned DataFrames
    combined = pd.concat(aligned_dfs, ignore_index=True)
    
    return combined

def apply_optimized_schema(df):
    """Apply consistent data types efficiently"""
    
    column_types = {
        "VendorID": "Int8",  
        "tpep_pickup_datetime": "datetime64[ns]",
        "tpep_dropoff_datetime": "datetime64[ns]",
        "passenger_count": "Int8",
        "trip_distance": "float32",  
        "RatecodeID": "Int8",
        "store_and_fwd_flag": "category",  
        "PULocationID": "Int16",
        "DOLocationID": "Int16",
        "payment_type": "Int8",
        "fare_amount": "float32",
        "extra": "float32",
        "mta_tax": "float32",
        "tip_amount": "float32",
        "tolls_amount": "float32",
        "improvement_surcharge": "float32",
        "total_amount": "float32",
        "congestion_surcharge": "float32",
        "airport_fee": "float32"
    }
    
    memory_before = df.memory_usage(deep=True).sum() / 1024**2
    
    for column, dtype in column_types.items():
        if column in df.columns:
            try:
                original_dtype = str(df[column].dtype)
                df[column] = robust_pandas_cast(df[column], dtype)
                new_dtype = str(df[column].dtype)
                
                if original_dtype != new_dtype:
                    print(f"  Cast {column}: {original_dtype} → {new_dtype}")
                    
            except Exception as e:
                print(f"  Warning: Failed to cast {column}: {e}")
    
    memory_after = df.memory_usage(deep=True).sum() / 1024**2
    print(f"Memory optimization: {memory_before:.1f}MB → {memory_after:.1f}MB")
    
    return df

def robust_pandas_cast(series, target_dtype):
    """Safely cast pandas series with appropriate fallbacks"""
    
    if target_dtype == "datetime64[ns]":
        return pd.to_datetime(series, errors='coerce')
    
    elif "Int" in target_dtype:
        # Handle nullable integers
        numeric_series = pd.to_numeric(series, errors='coerce')
        if "8" in target_dtype:
            return numeric_series.astype('float64').astype('Int8')
        elif "16" in target_dtype:
            return numeric_series.astype('float64').astype('Int16')
        else:
            return numeric_series.astype('float64').astype('Int64')
    
    elif "float" in target_dtype:
        return pd.to_numeric(series, errors='coerce').astype(target_dtype)
    
    elif target_dtype == "category":
        return series.astype('category')
    
    elif target_dtype == "string":
        return series.astype('string')
    
    else:
        return series.astype(target_dtype)

def transform_taxi_data(datasets):
    """Apply business transformations to type-consistent data"""
    
    processed = {}
    
    for dataset_name, df in datasets.items():
        print(f"Transforming {dataset_name}...")
        taxi_data = df.copy()
        
        # 1. Trip duration calculations (requires proper datetime types)
        taxi_data['trip_duration_minutes'] = (
            taxi_data['tpep_dropoff_datetime'] - taxi_data['tpep_pickup_datetime']
        ).dt.total_seconds() / 60
        
        # 2. Speed calculations (requires proper numeric types)
        taxi_data['average_speed_mph'] = taxi_data['trip_distance'] / (taxi_data['trip_duration_minutes'] / 60)
        taxi_data['average_speed_mph'] = taxi_data['average_speed_mph'].clip(0, 100)  # Reasonable bounds
        
        # 3. Time-based features (requires proper datetime types)
        taxi_data['pickup_hour'] = taxi_data['tpep_pickup_datetime'].dt.hour
        taxi_data['pickup_day_of_week'] = taxi_data['tpep_pickup_datetime'].dt.day_name()
        taxi_data['pickup_month'] = taxi_data['tpep_pickup_datetime'].dt.month
        taxi_data['pickup_year'] = taxi_data['tpep_pickup_datetime'].dt.year
        
        # 4. Time of day categories
        taxi_data['time_of_day'] = pd.cut(
            taxi_data['pickup_hour'], 
            bins=[0, 6, 12, 18, 24], 
            labels=['Night', 'Morning', 'Afternoon', 'Evening'],
            include_lowest=True
        )
        
        # 5. Business flags (requires proper integer types)
        taxi_data['is_airport_trip'] = taxi_data['RatecodeID'].isin([2, 3])
        taxi_data['is_credit_card_payment'] = taxi_data['payment_type'] == 1
        taxi_data['is_weekend'] = taxi_data['tpep_pickup_datetime'].dt.dayofweek >= 5
        
        # 6. Revenue segments (requires proper float types)
        taxi_data['revenue_segment'] = pd.cut(
            taxi_data['total_amount'], 
            bins=[0, 10, 20, 50, float('inf')],
            labels=['Low', 'Medium', 'High', 'Very High']
        )
        
        # 7. Tip analysis (requires proper float types)
        taxi_data['tip_percentage'] = (
            taxi_data['tip_amount'] / taxi_data['fare_amount'] * 100
        ).replace([float('inf'), -float('inf')], 0).fillna(0)
        
        # 8. Data validation flags
        taxi_data['is_valid_trip'] = (
            (taxi_data['trip_duration_minutes'] > 0) &
            (taxi_data['trip_duration_minutes'] < 180) &
            (taxi_data['trip_distance'] > 0) &
            (taxi_data['trip_distance'] < 100) &
            (taxi_data['fare_amount'] >= 0) &
            (taxi_data['total_amount'] >= 0)
        )
        
        processed[dataset_name] = taxi_data
        print(f"   Transformed {dataset_name}: {len(taxi_data):,} rows")
        print(f"   Valid trips: {taxi_data['is_valid_trip'].sum():,} rows")
    
    return processed

def create_taxi_metrics(processed_datasets):
    """Create business metrics from processed taxi data"""
    
    metrics = {}
    
    # Combine all datasets for comprehensive metrics
    all_trips = pd.concat(processed_datasets.values(), ignore_index=True)
    
    if len(all_trips) == 0:
        print("No data available for metrics calculation")
        return metrics
    
    # Filter valid trips only
    valid_trips = all_trips[all_trips['is_valid_trip'] == True]
    
    print(f"Calculating metrics from {len(valid_trips):,} valid trips...")
    
    # Report on date ranges in the actual data
    if 'pickup_year' in valid_trips.columns and 'pickup_month' in valid_trips.columns:
        actual_years = sorted(valid_trips['pickup_year'].unique())
        actual_months = sorted(valid_trips['pickup_month'].unique())
        print(f" Actual data years: {actual_years}, months: {actual_months}")
    
    # 1. Vendor Performance Metrics
    vendor_metrics = valid_trips.groupby('VendorID').agg({
        'trip_duration_minutes': ['count', 'mean', 'median'],
        'total_amount': ['sum', 'mean', 'median'],
        'tip_amount': ['sum', 'mean'],
        'trip_distance': ['mean', 'median']
    }).round(2)
    
    vendor_metrics.columns = ['trip_count', 'avg_duration', 'median_duration',
                             'total_revenue', 'avg_revenue', 'median_revenue',
                             'total_tips', 'avg_tips', 'avg_distance', 'median_distance']
    vendor_metrics = vendor_metrics.reset_index()
    metrics['vendor_performance'] = vendor_metrics
    
    # 2. Hourly Demand Patterns
    hourly_demand = valid_trips.groupby('pickup_hour').agg({
        'VendorID': 'count',
        'total_amount': 'sum',
        'trip_duration_minutes': 'mean'
    }).round(2)
    
    hourly_demand.columns = ['trip_count', 'total_revenue', 'avg_duration']
    hourly_demand = hourly_demand.reset_index()
    metrics['hourly_demand'] = hourly_demand
    
    # 3. Location Analysis
    pickup_analysis = valid_trips.groupby('PULocationID').agg({
        'VendorID': 'count',
        'total_amount': 'sum',
        'trip_duration_minutes': 'mean'
    }).round(2)
    
    pickup_analysis.columns = ['trip_count', 'total_revenue', 'avg_duration']
    pickup_analysis = pickup_analysis.reset_index()
    metrics['pickup_location_analysis'] = pickup_analysis
    
    # 4. Payment Type Analysis
    payment_analysis = valid_trips.groupby('payment_type').agg({
        'VendorID': 'count',
        'total_amount': 'sum',
        'tip_amount': 'sum',
        'tip_percentage': 'mean'
    }).round(2)
    
    payment_analysis.columns = ['trip_count', 'total_revenue', 'total_tips', 'avg_tip_percentage']
    payment_analysis = payment_analysis.reset_index()
    metrics['payment_analysis'] = payment_analysis
    
    # 5. Monthly Summary
    if 'pickup_month' in valid_trips.columns:
        monthly_summary = valid_trips.groupby(['pickup_year', 'pickup_month']).agg({
            'VendorID': 'count',
            'total_amount': 'sum',
            'trip_duration_minutes': 'mean',
            'trip_distance': 'mean'
        }).round(2)
        
        monthly_summary.columns = ['trip_count', 'total_revenue', 'avg_duration', 'avg_distance']
        monthly_summary = monthly_summary.reset_index()
        metrics['monthly_summary'] = monthly_summary
        
        # Report what's actually in the monthly summary
        if not monthly_summary.empty:
            summary_years = sorted(monthly_summary['pickup_year'].unique())
            summary_months = sorted(monthly_summary['pickup_month'].unique())
            print(f" Monthly summary includes: years {summary_years}, months {summary_months}")
    
    print(f" Created {len(metrics)} business metrics datasets")
    
    return metrics

if __name__ == "__main__":
    success = process_incremental_files()
    
    if success:
        print("\n" + "="*60)
        print(" INCREMENTAL PROCESSING COMPLETED SUCCESSFULLY!")
        print(" Only processed NEW files since last run")
        print("  Watermark + Idempotent - safe to rerun anytime!")
        print(" Next run will only process files after the watermark")
    else:
        print("\n" + "="*60)
        print(" PROCESSING FAILED - Check errors above")