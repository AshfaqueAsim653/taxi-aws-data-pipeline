import pandas as pd
import boto3
import os
import tempfile
from datetime import datetime, timezone
import numpy as np
import json
from dotenv import load_dotenv
import re
from prefect import flow, task, get_run_logger
from prefect.server.schemas.schedules import CronSchedule
from typing import Dict, List, Optional, Tuple

import sys
from prefect import flow
from prefect.client.schemas.schedules import IntervalSchedule


# Watermark storage in S3
WATERMARK_KEY = "processed/_metadata/last_processed_watermark.txt"
PROCESSED_TRACKER_KEY = "processed/_metadata/processed_files.json"

@task(
    name="Load AWS Credentials",
    description="Load AWS credentials from environment variables",
    retries=2,
    retry_delay_seconds=30,
    cache_policy=None
)
def load_aws_credentials():
    """Load AWS credentials from environment variables"""
    logger = get_run_logger()
    try:
        # Load environment variables
        load_dotenv()
        credentials = {
            "aws_access_key_id": os.getenv('AWS_ACCESS_KEY_ID'),
            "aws_secret_access_key": os.getenv('AWS_SECRET_ACCESS_KEY'),
            "region_name": os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
        }
        
        # Validate credentials
        if not credentials["aws_access_key_id"] or not credentials["aws_secret_access_key"]:
            raise ValueError("AWS credentials not found in environment variables")
            
        logger.info(" Loaded AWS credentials from environment variables")
        return credentials
    except Exception as e:
        logger.error(f" Failed to load AWS credentials: {e}")
        raise

@task(
    name="Load Configuration",
    description="Load pipeline configuration",
    cache_policy=None
)
def load_configuration():
    """Load pipeline configuration"""
    logger = get_run_logger()
    try:
        # Load from environment or use defaults
        config = {
            "bucket_name": os.getenv('AWS_S3_BUCKET_NAME', 'nyc-tlc-taxi-data'),
            "region": os.getenv('AWS_DEFAULT_REGION', 'ap-south-1'),
            "max_files_per_run": int(os.getenv('MAX_FILES_PER_RUN', '10')),
            "processing_batch_size": int(os.getenv('PROCESSING_BATCH_SIZE', '2'))
        }
        logger.info(" Loaded pipeline configuration")
        return config
    except Exception as e:
        logger.error(f" Failed to load configuration: {e}")
        raise

@task(
    name="Get Processing Watermark",
    description="Get last processed timestamp from S3",
    cache_policy=None
)
def get_processing_watermark(s3_client, bucket_name: str) -> datetime:
    """Get the last processed timestamp watermark"""
    logger = get_run_logger()
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=WATERMARK_KEY)
        watermark_str = response['Body'].read().decode('utf-8').strip()
        watermark = datetime.fromisoformat(watermark_str)
        logger.info(f" Retrieved watermark: {watermark}")
        return watermark
    except s3_client.exceptions.NoSuchKey:
        # First run - start from beginning of time
        watermark = datetime(2000, 1, 1, tzinfo=timezone.utc)
        logger.info(f" First run, using default watermark: {watermark}")
        return watermark
    except Exception as e:
        logger.warning(f" Could not read watermark, using default: {e}")
        return datetime(2000, 1, 1, tzinfo=timezone.utc)

@task(
    name="Get Processed Files Tracker",
    description="Get set of already processed files from S3",
    cache_policy=None
)
def get_processed_files_tracker(s3_client, bucket_name: str) -> set:
    """Get the set of already processed files"""
    logger = get_run_logger()
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=PROCESSED_TRACKER_KEY)
        processed_data = json.loads(response['Body'].read().decode('utf-8'))
        processed_files = set(processed_data.get('processed_files', []))
        logger.info(f" Retrieved {len(processed_files)} processed files from tracker")
        return processed_files
    except s3_client.exceptions.NoSuchKey:
        logger.info(" No processed files tracker found")
        return set()
    except Exception as e:
        logger.warning(f" Could not read processed files tracker: {e}")
        return set()

@task(
    name="Find Files Since Watermark",
    description="Discover new files in S3 since last watermark",
    cache_policy=None
)
def find_files_since_watermark(s3_client, bucket_name: str, watermark: datetime) -> List[Dict]:
    """Find all files modified since the watermark"""
    logger = get_run_logger()
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
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
        
        logger.info(f" Found {len(new_files)} new files since watermark")
        return new_files
        
    except Exception as e:
        logger.error(f" Error finding files: {e}")
        return []

@task(
    name="Deduplicate Files",
    description="Remove duplicate files, keeping only the most recent version",
    cache_policy=None
)
def deduplicate_files(file_objects: List[Dict]) -> List[Dict]:
    """Remove duplicate files, keeping only the most recent version of each file"""
    logger = get_run_logger()
    unique_files = {}
    
    for file_obj in file_objects:
        filename = os.path.basename(file_obj['Key'])
        
        # If we haven't seen this filename, or this one is newer, keep it
        if filename not in unique_files or file_obj['LastModified'] > unique_files[filename]['LastModified']:
            unique_files[filename] = file_obj
    
    deduped_files = list(unique_files.values())
    logger.info(f" Deduplication: {len(file_objects)} → {len(deduped_files)} files")
    return deduped_files

@task(
    name="Extract Year Month from Filename",
    description="Extract year and month from dataset filename",
    cache_policy=None
)
def extract_year_month_from_dataset_name(dataset_name: str) -> Optional[Tuple[int, int]]:
    """Extract year and month from dataset filename for validation"""
    # Pattern to match yellow_tripdata_YYYY-MM.parquet
    pattern = r'yellow_tripdata_(\d{4})-(\d{2})\.parquet'
    match = re.search(pattern, dataset_name)
    
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        return year, month
    return None

@task(
    name="Validate Data Against Filename",
    description="Validate that data matches the expected year/month from filename",
    cache_policy=None
)
def validate_data_against_filename(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """Validate that data matches the expected year/month from filename"""
    logger = get_run_logger()
    expected_year_month = extract_year_month_from_dataset_name(dataset_name)
    
    if not expected_year_month:
        logger.warning(f" Could not extract year/month from filename: {dataset_name}")
        return df
    
    expected_year, expected_month = expected_year_month
    logger.info(f"📅 Validating data against expected: {expected_year}-{expected_month:02d}")
    
    # Check if we have the required datetime columns
    if 'tpep_pickup_datetime' not in df.columns:
        logger.warning(" Missing datetime columns for validation")
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
        
        logger.warning(f" Found {mismatched_count:,} mismatched records")
        logger.warning(f" Unexpected years: {unexpected_years}")
        logger.warning(f" Unexpected months: {unexpected_months}")
        
        # Filter out mismatched records
        df = df[~mismatched_mask]
        logger.info(f" Filtered out {mismatched_count:,} mismatched records")
        logger.info(f" Remaining records: {len(df):,}")
    else:
        logger.info(" All records match expected year/month")
    
    # Clean up temporary columns
    df = df.drop(['data_pickup_year', 'data_pickup_month'], axis=1, errors='ignore')
    
    return df

@task(
    name="Clean Taxi Data",
    description="Comprehensive data cleaning for taxi data",
    retries=2,
    retry_delay_seconds=60,
    cache_policy=None
)
def clean_taxi_data(df: pd.DataFrame) -> pd.DataFrame:
    """Comprehensive data cleaning for taxi data"""
    logger = get_run_logger()
    
    logger.info(" Starting data cleaning...")
    original_rows = len(df)
    
    # Create a copy to avoid modifying the original
    df_cleaned = df.copy()
    
    # 1. Handle missing values
    logger.info("   Handling missing values...")
    
    # Numeric columns - fill with median
    numeric_columns = ['passenger_count', 'trip_distance', 'fare_amount', 'extra', 
                      'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
                      'total_amount', 'congestion_surcharge', 'airport_fee']
    
    for col in numeric_columns:
        if col in df_cleaned.columns:
            missing_before = df_cleaned[col].isna().sum()
            if missing_before > 0:
                df_cleaned[col] = df_cleaned[col].fillna(df_cleaned[col].median())
                logger.info(f"     Filled {missing_before:,} missing values in {col}")
    
    # Categorical columns - fill with mode
    categorical_columns = ['store_and_fwd_flag', 'RatecodeID', 'payment_type']
    for col in categorical_columns:
        if col in df_cleaned.columns and df_cleaned[col].notna().any():
            missing_before = df_cleaned[col].isna().sum()
            if missing_before > 0:
                mode_val = df_cleaned[col].mode().iloc[0] if not df_cleaned[col].mode().empty else 'Unknown'
                df_cleaned[col] = df_cleaned[col].fillna(mode_val)
                logger.info(f"     Filled {missing_before:,} missing values in {col}")
    
    # Location IDs - fill with 0 (unknown location)
    location_columns = ['PULocationID', 'DOLocationID']
    for col in location_columns:
        if col in df_cleaned.columns:
            missing_before = df_cleaned[col].isna().sum()
            if missing_before > 0:
                df_cleaned[col] = df_cleaned[col].fillna(0)
                logger.info(f"     Filled {missing_before:,} missing values in {col}")
    
    # 2. Remove duplicate records
    logger.info("   Removing duplicate records...")
    duplicates_before = df_cleaned.duplicated().sum()
    if duplicates_before > 0:
        df_cleaned = df_cleaned.drop_duplicates()
        logger.info(f"     Removed {duplicates_before:,} duplicate records")
    
    # 3. Handle outliers and invalid values
    logger.info("   Handling outliers and invalid values...")
    
    # Trip distance - reasonable bounds (0 to 100 miles)
    if 'trip_distance' in df_cleaned.columns:
        outlier_mask = (df_cleaned['trip_distance'] <= 0) | (df_cleaned['trip_distance'] > 100)
        outliers_before = outlier_mask.sum()
        if outliers_before > 0:
            p99 = df_cleaned['trip_distance'].quantile(0.99)
            df_cleaned.loc[outlier_mask, 'trip_distance'] = p99
            logger.info(f"     Capped {outliers_before:,} outlier trip distances")
    
    # Fare amount - reasonable bounds (0 to $500)
    if 'fare_amount' in df_cleaned.columns:
        outlier_mask = (df_cleaned['fare_amount'] < 0) | (df_cleaned['fare_amount'] > 500)
        outliers_before = outlier_mask.sum()
        if outliers_before > 0:
            p99 = df_cleaned['fare_amount'][df_cleaned['fare_amount'] >= 0].quantile(0.99)
            df_cleaned.loc[outlier_mask, 'fare_amount'] = p99
            logger.info(f"     Capped {outliers_before:,} outlier fare amounts")
    
    # Passenger count - reasonable bounds (1 to 6)
    if 'passenger_count' in df_cleaned.columns:
        outlier_mask = (df_cleaned['passenger_count'] < 1) | (df_cleaned['passenger_count'] > 6)
        outliers_before = outlier_mask.sum()
        if outliers_before > 0:
            df_cleaned.loc[df_cleaned['passenger_count'] < 1, 'passenger_count'] = 1
            df_cleaned.loc[df_cleaned['passenger_count'] > 6, 'passenger_count'] = 6
            logger.info(f"     Fixed {outliers_before:,} outlier passenger counts")
    
    # 4. Data consistency fixes
    logger.info("   Applying data consistency fixes...")
    
    # Ensure total_amount is consistent with component sum
    if all(col in df_cleaned.columns for col in ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 
                                               'tolls_amount', 'improvement_surcharge', 
                                               'congestion_surcharge', 'airport_fee', 'total_amount']):
        component_sum = (df_cleaned['fare_amount'] + df_cleaned['extra'] + df_cleaned['mta_tax'] + 
                        df_cleaned['tip_amount'] + df_cleaned['tolls_amount'] + 
                        df_cleaned['improvement_surcharge'] + df_cleaned['congestion_surcharge'] + 
                        df_cleaned['airport_fee'])
        
        amount_discrepancy = abs(df_cleaned['total_amount'] - component_sum) > 0.1
        discrepancies = amount_discrepancy.sum()
        if discrepancies > 0:
            df_cleaned.loc[amount_discrepancy, 'total_amount'] = component_sum
            logger.info(f"     Fixed {discrepancies:,} total amount discrepancies")
    
    # Fix datetime inconsistencies (dropoff before pickup)
    if all(col in df_cleaned.columns for col in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']):
        time_inconsistencies = (df_cleaned['tpep_dropoff_datetime'] < df_cleaned['tpep_pickup_datetime']).sum()
        if time_inconsistencies > 0:
            mask = df_cleaned['tpep_dropoff_datetime'] < df_cleaned['tpep_pickup_datetime']
            df_cleaned.loc[mask, 'tpep_dropoff_datetime'] = (
                df_cleaned.loc[mask, 'tpep_pickup_datetime'] + pd.Timedelta(minutes=5)
            )
            logger.info(f"     Fixed {time_inconsistencies:,} datetime inconsistencies")
    
    # 5. Remove clearly invalid records that can't be fixed
    logger.info("  🔍 Removing invalid records...")
    
    invalid_mask = (
        (df_cleaned['fare_amount'] <= 0) |
        (df_cleaned['trip_distance'] <= 0) |
        (df_cleaned['tpep_pickup_datetime'].isna()) |
        (df_cleaned['tpep_dropoff_datetime'].isna())
    )
    
    invalid_count = invalid_mask.sum()
    if invalid_count > 0:
        df_cleaned = df_cleaned[~invalid_mask]
        logger.info(f"     Removed {invalid_count:,} fundamentally invalid records")
    
    # Final summary
    cleaned_rows = len(df_cleaned)
    rows_removed = original_rows - cleaned_rows
    cleaning_rate = (rows_removed / original_rows) * 100
    
    logger.info(f"   Cleaning complete: {original_rows:,} → {cleaned_rows:,} rows")
    logger.info(f"   Removed {rows_removed:,} rows ({cleaning_rate:.2f}%)")
    
    return df_cleaned

@task(
    name="Load Files with Memory Optimization",
    description="Load Parquet files with memory-efficient settings",
    timeout_seconds=600,
    cache_policy=None
)
def load_files_with_memory_optimization(s3_client, bucket_name: str, s3_keys: List[str]) -> Dict[str, pd.DataFrame]:
    """Load files with memory-efficient settings"""
    logger = get_run_logger()
    datasets = {}
    
    for s3_key in s3_keys:
        try:
            file_name = os.path.basename(s3_key)
            logger.info(f"Loading {file_name}...")
            
            # Download to temporary file
            local_path = os.path.join(tempfile.gettempdir(), file_name)
            s3_client.download_file(bucket_name, s3_key, local_path)
            
            # Load with optimized settings for large files
            df = pd.read_parquet(
                local_path,
                engine='pyarrow',
                use_pandas_metadata=True
            )
            
            # Validate data against filename
            df = validate_data_against_filename(df, file_name)
            
            # Optimize memory usage
            df = optimize_dataframe_memory(df)
            
            datasets[file_name] = df
            logger.info(f"   Loaded: {len(df):,} rows, {len(df.columns)} columns")
            logger.info(f"   Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
            
            # Clean up
            os.remove(local_path)
            
        except Exception as e:
            logger.error(f"   Failed to load {file_name}: {e}")
    
    return datasets

@task(
    name="Optimize DataFrame Memory",
    description="Optimize DataFrame memory usage",
    cache_policy=None
)
def optimize_dataframe_memory(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize DataFrame memory usage"""
    # Convert object columns to category if they have low cardinality
    for col in df.select_dtypes(include=['object']):
        if df[col].nunique() / len(df) < 0.5:
            df[col] = df[col].astype('category')
    
    # Downcast numeric columns
    for col in df.select_dtypes(include=['int64']):
        df[col] = pd.to_numeric(df[col], downcast='integer')
    
    for col in df.select_dtypes(include=['float64']):
        df[col] = pd.to_numeric(df[col], downcast='float')
    
    return df

@task(
    name="Efficient Union DataFrames",
    description="Efficiently combine DataFrames with schema handling",
    cache_policy=None
)
def efficient_union_dataframes(datasets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Efficiently combine DataFrames with schema handling"""
    logger = get_run_logger()
    
    if not datasets:
        return pd.DataFrame()
    
    dataframes = list(datasets.values())
    
    # Get all unique columns across all DataFrames
    all_columns = set()
    for df in dataframes:
        all_columns.update(df.columns)
    
    logger.info(f"Columns across all files: {list(all_columns)}")
    
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

@task(
    name="Apply Optimized Schema",
    description="Apply consistent data types efficiently",
    cache_policy=None
)
def apply_optimized_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Apply consistent data types efficiently"""
    logger = get_run_logger()
    
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
                    logger.info(f"  Cast {column}: {original_dtype} → {new_dtype}")
                    
            except Exception as e:
                logger.warning(f"  Warning: Failed to cast {column}: {e}")
    
    memory_after = df.memory_usage(deep=True).sum() / 1024**2
    logger.info(f"Memory optimization: {memory_before:.1f}MB → {memory_after:.1f}MB")
    
    return df

@task(
    name="Robust Pandas Cast",
    description="Safely cast pandas series with appropriate fallbacks",
    cache_policy=None
)
def robust_pandas_cast(series, target_dtype):
    """Safely cast pandas series with appropriate fallbacks"""
    if target_dtype == "datetime64[ns]":
        return pd.to_datetime(series, errors='coerce')
    
    elif "Int" in target_dtype:
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

@task(
    name="Transform Taxi Data",
    description="Apply business transformations to type-consistent data",
    timeout_seconds=300,
    cache_policy=None
)
def transform_taxi_data(cleaned_data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Apply business transformations to type-consistent data"""
    logger = get_run_logger()
    processed = {}
    
    logger.info("Transforming combined data...")
    taxi_data = cleaned_data.copy()
    
    # 1. Trip duration calculations
    taxi_data['trip_duration_minutes'] = (
        taxi_data['tpep_dropoff_datetime'] - taxi_data['tpep_pickup_datetime']
    ).dt.total_seconds() / 60
    
    # 2. Speed calculations
    taxi_data['average_speed_mph'] = taxi_data['trip_distance'] / (taxi_data['trip_duration_minutes'] / 60)
    taxi_data['average_speed_mph'] = taxi_data['average_speed_mph'].clip(0, 100)
    
    # 3. Time-based features
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
    
    # 5. Business flags
    taxi_data['is_airport_trip'] = taxi_data['RatecodeID'].isin([2, 3])
    taxi_data['is_credit_card_payment'] = taxi_data['payment_type'] == 1
    taxi_data['is_weekend'] = taxi_data['tpep_pickup_datetime'].dt.dayofweek >= 5
    
    # 6. Revenue segments
    taxi_data['revenue_segment'] = pd.cut(
        taxi_data['total_amount'], 
        bins=[0, 10, 20, 50, float('inf')],
        labels=['Low', 'Medium', 'High', 'Very High']
    )
    
    # 7. Tip analysis
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
    
    processed['combined'] = taxi_data
    logger.info(f"✓ Transformed combined data: {len(taxi_data):,} rows")
    logger.info(f"✓ Valid trips: {taxi_data['is_valid_trip'].sum():,} rows")
    
    return processed

@task(
    name="Create Taxi Metrics",
    description="Create business metrics from processed taxi data",
    cache_policy=None
)
def create_taxi_metrics(processed_datasets: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Create business metrics from processed taxi data"""
    logger = get_run_logger()
    metrics = {}
    
    # Combine all datasets for comprehensive metrics
    all_trips = pd.concat(processed_datasets.values(), ignore_index=True)
    
    if len(all_trips) == 0:
        logger.warning("No data available for metrics calculation")
        return metrics
    
    # Filter valid trips only
    valid_trips = all_trips[all_trips['is_valid_trip'] == True]
    
    logger.info(f"Calculating metrics from {len(valid_trips):,} valid trips...")
    
    # Report on date ranges in the actual data
    if 'pickup_year' in valid_trips.columns and 'pickup_month' in valid_trips.columns:
        actual_years = sorted(valid_trips['pickup_year'].unique())
        actual_months = sorted(valid_trips['pickup_month'].unique())
        logger.info(f" Actual data years: {actual_years}, months: {actual_months}")
    
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
            logger.info(f" Monthly summary includes: years {summary_years}, months {summary_months}")
    
    logger.info(f"✓ Created {len(metrics)} business metrics datasets")
    return metrics

@task(
    name="Upload Processed Data",
    description="Upload processed taxi data and metrics to S3 with versioning",
    timeout_seconds=600,
    cache_policy=None
)
def upload_processed_taxi_data_versioned(s3_client, bucket_name: str, processed_datasets: Dict, 
                                       business_metrics: Dict, processing_id: str, date_range: str) -> bool:
    """Upload processed taxi data and metrics to S3 with versioning"""
    logger = get_run_logger()
    upload_count = 0
    
    # Upload processed datasets with versioning
    for dataset_name, df in processed_datasets.items():
        try:
            # Save as parquet file
            local_path = os.path.join(tempfile.gettempdir(), f"{dataset_name}.parquet")
            df.to_parquet(local_path, index=False, compression='snappy')
            
            # Upload to S3 with versioning
            s3_key = f"processed/taxi/{processing_id}/{dataset_name}.parquet"
            s3_client.upload_file(local_path, bucket_name, s3_key)
            
            # Also create a latest symlink for easy access
            latest_key = f"processed/taxi/latest/{dataset_name}.parquet"
            try:
                s3_client.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': s3_key},
                    Key=latest_key
                )
                logger.info(f" Uploaded {dataset_name}: {len(df):,} records")
                logger.info(f"   Versioned: {s3_key}")
                logger.info(f"   Latest: {latest_key}")
            except Exception as copy_error:
                logger.info(f" Uploaded {dataset_name}: {len(df):,} records")
                logger.info(f"   Versioned: {s3_key}")
                logger.info(f"    Latest link failed: {copy_error}")
            
            upload_count += 1
            
            # Clean up
            os.remove(local_path)
            
        except Exception as e:
            logger.error(f" Failed to upload {dataset_name}: {e}")
    
    # Upload business metrics with date-based partitioning
    for metric_name, df in business_metrics.items():
        try:
            # Save as CSV for metrics
            local_path = os.path.join(tempfile.gettempdir(), f"{metric_name}.csv")
            df.to_csv(local_path, index=False)
            
            # Upload to S3 with date versioning
            s3_key = f"processed/metrics/date={date_range}/{processing_id}_{metric_name}.csv"
            s3_client.upload_file(local_path, bucket_name, s3_key)
            
            # Also create latest version
            latest_key = f"processed/metrics/latest/{metric_name}.csv"
            try:
                s3_client.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': s3_key},
                    Key=latest_key
                )
                logger.info(f" Uploaded {metric_name}: {len(df)} records")
                logger.info(f"   Versioned: {s3_key}")
                logger.info(f"  🔗 Latest: {latest_key}")
            except Exception as copy_error:
                logger.info(f" Uploaded {metric_name}: {len(df)} records")
                logger.info(f"   Versioned: {s3_key}")
                logger.info(f"    Latest link failed: {copy_error}")
            
            upload_count += 1
            
            # Clean up
            os.remove(local_path)
            
        except Exception as e:
            logger.error(f"✗ Failed to upload {metric_name}: {e}")
    
    total_files = len(processed_datasets) + len(business_metrics)
    success = upload_count == total_files
    
    if success:
        logger.info(f" Successfully uploaded all {upload_count} files to S3!")
        logger.info(f" Processing ID: {processing_id}")
        logger.info(f" Date range: {date_range}")
    else:
        logger.warning(f" Partial upload: {upload_count}/{total_files} files")
    
    return success

@task(
    name="Generate Processing Info",
    description="Extract date range from S3 filenames for versioning",
    cache_policy=None
)
def generate_processing_info_from_filenames(parquet_files: List[str]) -> Tuple[str, str]:
    """Extract date range from S3 filenames for versioning"""
    logger = get_run_logger()
    
    months = []
    for s3_key in parquet_files:
        filename = os.path.basename(s3_key)
        if 'yellow_tripdata_' in filename:
            date_part = filename.replace('yellow_tripdata_', '').replace('.parquet', '')
            months.append(date_part)
    
    if months:
        months.sort()
        date_range = f"{months[0]}_{months[-1]}"
    else:
        current_date = datetime.now().strftime("%Y-%m")
        date_range = f"{current_date}_{current_date}"
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    processing_id = f"proc_{timestamp}"
    
    logger.info(f" Date range from filenames: {date_range}")
    logger.info(f" Processing ID: {processing_id}")
    
    return processing_id, date_range

@task(
    name="Mark Files Processed",
    description="Mark files as processed in the tracker",
    cache_policy=None
)
def mark_files_processed(s3_client, bucket_name: str, file_keys: List[str]):
    """Mark files as processed in the tracker"""
    logger = get_run_logger()
    try:
        processed_files = get_processed_files_tracker(s3_client, bucket_name)
        processed_files.update(file_keys)
        
        # Keep only last 1000 files to avoid infinite growth
        if len(processed_files) > 1000:
            processed_files = set(list(processed_files)[-1000:])
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=PROCESSED_TRACKER_KEY,
            Body=json.dumps({'processed_files': list(processed_files)})
        )
        logger.info(f" Marked {len(file_keys)} files as processed")
    except Exception as e:
        logger.error(f" Could not update processed files tracker: {e}")
        raise

@task(
    name="Update Processing Watermark",
    description="Update the watermark to track progress",
    cache_policy=None
)
def update_processing_watermark(s3_client, bucket_name: str, new_watermark: datetime):
    """Update the watermark to track progress"""
    logger = get_run_logger()
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=WATERMARK_KEY,
            Body=new_watermark.isoformat().encode('utf-8')
        )
        logger.info(f" Updated watermark: {new_watermark}")
    except Exception as e:
        logger.error(f" Could not update watermark: {e}")
        raise

@flow(
    name="incremental-taxi-data-processing",
    description="Process incremental NYC taxi data from S3 with watermark tracking",
    log_prints=True
)
def incremental_taxi_data_processing():
    """Prefect flow for incremental taxi data processing"""
    logger = get_run_logger()
    
    logger.info(" Starting Incremental Taxi Data Processing Flow...")
    
    try:
        # Load configuration and credentials
        aws_creds = load_aws_credentials()
        config = load_configuration()
        
        bucket_name = config.get("bucket_name", "nyc-tlc-taxi-data")
        region = config.get("region", "ap-south-1")
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_creds["aws_access_key_id"],
            aws_secret_access_key=aws_creds["aws_secret_access_key"],
            region_name=region
        )
        
        # Step 1: Get current state
        watermark = get_processing_watermark(s3_client, bucket_name)
        processed_files = get_processed_files_tracker(s3_client, bucket_name)
        
        # Step 2: Discover new files
        new_files = find_files_since_watermark(s3_client, bucket_name, watermark)
        
        if not new_files:
            logger.info(" No new files to process")
            return {"status": "success", "files_processed": 0, "message": "No new files"}
        
        # Step 3: Deduplicate files
        unique_files = deduplicate_files(new_files)
        
        # Step 4: Filter out already processed files
        files_to_process = [
            file_obj for file_obj in unique_files 
            if file_obj['Key'] not in processed_files
        ]
        
        if not files_to_process:
            logger.info(" All new files have already been processed")
            # Update watermark to current time
            update_processing_watermark(s3_client, bucket_name, datetime.now(timezone.utc))
            return {"status": "success", "files_processed": 0, "message": "All files already processed"}
        
        file_keys = [file_obj['Key'] for file_obj in files_to_process]
        logger.info(f" Processing {len(file_keys)} new files")
        
        # Step 5: Process the files
        datasets = load_files_with_memory_optimization(s3_client, bucket_name, file_keys)
        
        if not datasets:
            logger.error(" Failed to load any datasets")
            return {"status": "failed", "files_processed": 0, "message": "No datasets loaded"}
        
        # Step 6: Combine and process data
        combined_data = efficient_union_dataframes(datasets)
        processed_data = apply_optimized_schema(combined_data)
        cleaned_data = clean_taxi_data(processed_data)
        transformed_data = transform_taxi_data(cleaned_data)
        business_metrics = create_taxi_metrics(transformed_data)
        
        # Step 7: Upload results
        processing_id, date_range = generate_processing_info_from_filenames(file_keys)
        upload_success = upload_processed_taxi_data_versioned(
            s3_client, bucket_name, transformed_data, business_metrics, processing_id, date_range
        )
        
        # Step 8: Update tracking if successful
        if upload_success:
            mark_files_processed(s3_client, bucket_name, file_keys)
            
            # Update watermark to the latest processed file timestamp
            latest_timestamp = max(obj['LastModified'] for obj in files_to_process)
            update_processing_watermark(s3_client, bucket_name, latest_timestamp)
            
            result = {
                "status": "success",
                "files_processed": len(file_keys),
                "watermark_updated": latest_timestamp.isoformat(),
                "processing_id": processing_id,
                "date_range": date_range,
                "message": f"Successfully processed {len(file_keys)} files"
            }
            logger.info(f"🎉 Flow completed successfully: {result}")
        else:
            result = {
                "status": "failed",
                "files_processed": 0,
                "message": "Data upload failed"
            }
            logger.error(" Flow failed during data upload")
        
        return result
        
    except Exception as e:
        logger.error(f" Flow execution failed: {e}")
        return {
            "status": "failed",
            "files_processed": 0,
            "message": f"Flow execution failed: {str(e)}"
        }

if __name__ == "__main__":
    import sys
    from prefect.server.schemas.schedules import IntervalSchedule

    if len(sys.argv) > 1 and sys.argv[1] == "deploy":
        print(" Creating Prefect deployments...")

        # 1. Manual
        incremental_taxi_data_processing.deploy(
            name="taxi-manual-run",
            work_pool_name="project-pool",
            storage="prefect",   # <-- REQUIRED
            tags=["manual", "taxi"],
        )

        # 2. Daily
        incremental_taxi_data_processing.deploy(
            name="taxi-incremental-daily",
            work_pool_name="project-pool",
            schedule=IntervalSchedule(interval=86400),
            storage="prefect",   # <-- REQUIRED
            tags=["daily", "taxi"],
        )

        # 3. Hourly
        incremental_taxi_data_processing.deploy(
            name="taxi-incremental-hourly",
            work_pool_name="project-pool",
            schedule=IntervalSchedule(interval=3600),
            storage="prefect",   # <-- REQUIRED
            tags=["hourly", "taxi"],
        )

        print(" Deployments created successfully!")
        print(" Check at: http://127.0.0.1:4200/deployments")


