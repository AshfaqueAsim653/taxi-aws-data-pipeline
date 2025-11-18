# Taxi AWS Data Pipeline

## Architecture
![Architecture Diagram](link-to-your-architecture-image)  
*Illustration showing Prefect orchestration, Dockerized services, S3 storage, and batch/incremental flows.*

---

# Motivation

The goal of this project is to gain hands-on experience with modern data engineering tools such as **Python, Docker, Prefect, and AWS S3** by building a fully functional **incremental taxi data pipeline**.

To make the pipeline robust and production-ready:

- **Data deduplication and batch processing** implemented in Python  
- **Orchestration** done using Prefect flows  
- **Containerized** with Docker and managed via Docker Compose  
- Supports **incremental and manual flow runs** for flexibility  
- **Logging** implemented for monitoring pipeline health and process status  
- **Watermarking** used to avoid re-processing old data


---

# Dataset

**Source:** NYC Taxi and Limousine Commission  
[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

**Variables Extracted:**

- `VendorID`: A code indicating the TPEP provider that provided the record.  
  - `1` = Creative Mobile Technologies, LLC  
  - `2` = Curb Mobility, LLC  
  - `6` = Myle Technologies Inc  
  - `7` = Helix

- `tpep_pickup_datetime`: Date and time when the meter was engaged.  
- `tpep_dropoff_datetime`: Date and time when the meter was disengaged.  
- `passenger_count`: Number of passengers in the vehicle.  
- `trip_distance`: Trip distance in miles as reported by the taximeter.  
- `RatecodeID`: Final rate code at the end of the trip.  
  - `1` = Standard rate  
  - `2` = JFK  
  - `3` = Newark  
  - `4` = Nassau or Westchester  
  - `5` = Negotiated fare  
  - `6` = Group ride  
  - `99` = Null/unknown

- `store_and_fwd_flag`: Indicates whether the trip record was held in vehicle memory before sending to the vendor.  
  - `Y` = Store and forward trip  
  - `N` = Not a store and forward trip

- `PULocationID`: TLC Taxi Zone where the taximeter was engaged (pickup).  
- `DOLocationID`: TLC Taxi Zone where the taximeter was disengaged (dropoff).  
- `payment_type`: Numeric code indicating payment method.  
  - `0` = Flex Fare trip  
  - `1` = Credit card  
  - `2` = Cash  
  - `3` = No charge  
  - `4` = Dispute  
  - `5` = Unknown  
  - `6` = Voided trip

- `fare_amount`: Time-and-distance fare calculated by the meter.  
- `extra`: Miscellaneous extras and surcharges.  
- `mta_tax`: Automatically triggered tax based on the metered rate.  
- `tip_amount`: Tip amount (automatically populated for credit card tips; cash tips not included).  
- `tolls_amount`: Total tolls paid during the trip.  
- `improvement_surcharge`: Surcharge assessed at flag drop (introduced in 2015).  
- `total_amount`: Total amount charged to passengers (does **not** include cash tips).  
- `congestion_surcharge`: Total collected for NYS congestion surcharge.  
- `airport_fee`: For pickups at LaGuardia or JFK airports only.


---

## Project Summary
The Taxi AWS Data Pipeline consists of:

# Incremental Taxi Data Processing Flow

## 1️⃣ Flow Entry Point

**Prefect Flow:** `incremental_taxi_data_processing`  
**Purpose:** Orchestrates the full ETL pipeline.  
**Logging:** Starts with a log: `Starting Incremental Taxi Data Processing Flow...`

---

## 2️⃣ Load Configuration & Credentials

**Tasks:**

- `load_aws_credentials` → Loads AWS credentials from environment variables.  
- `load_configuration` → Loads pipeline settings like:
  - S3 bucket name (`nyc-tlc-taxi-data`)
  - Region
  - Batch size & max files per run  

**Outcome:** You have `aws_creds` and `config` to create the S3 client.

---

## 3️⃣ Connect to S3

**Code:** `boto3.client(...)`  
**Purpose:** To list, download, and upload files to S3.

---

## 4️⃣ Fetch Current Pipeline State

**Tasks:**

- `get_processing_watermark` → Gets the last processed timestamp from S3 (`processed/_metadata/last_processed_watermark.txt`).  
  - If missing → default: `2000-01-01`.
- `get_processed_files_tracker` → Loads a JSON from S3 tracking already processed files (`processed/_metadata/processed_files.json`).

**Outcome:** You know which files have already been processed and from which point to process new files.

---

## 5️⃣ Discover New Files in S3

**Task:** `find_files_since_watermark`  

- Lists S3 objects under `raw-data/`.
- Filters files:
  - Name contains `yellow_tripdata_`
  - Ends with `.parquet`
  - Last modified after the watermark
- Sorts files by `LastModified`.

---

## 6️⃣ Deduplicate Files

**Task:** `deduplicate_files`  
- If multiple files with the same filename exist, only keep the most recent one.

---

## 7️⃣ Filter Already Processed Files

- Compare deduplicated files with `processed_files`.  
- Only files not yet processed move forward.

---

## 8️⃣ Load Files Efficiently

**Task:** `load_files_with_memory_optimization`  

- Downloads each S3 file to a temp folder.  
- Reads parquet using `pyarrow`.  
- Validates data matches expected year/month from the filename (`validate_data_against_filename`).  
- Optimizes memory (`optimize_dataframe_memory`):
  - Converts strings to categories
  - Downcasts numeric columns  

**Outcome:** Dictionary of `filename -> pd.DataFrame`.

---

## 9️⃣ Combine Datasets

**Task:** `efficient_union_dataframes`  

- Aligns all dataframes to the same schema.  
- Adds missing columns as `None`.  
- Concatenates all files into a single dataframe.

---

## 🔟 Apply Consistent Schema

**Task:** `apply_optimized_schema`  

- Enforces explicit types (e.g., `Int8`, `float32`, `datetime64[ns]`, `category`).  
- Uses `robust_pandas_cast` for safe conversions.  

**Outcome:** A memory-optimized, type-consistent combined dataframe.

---

## 1️⃣1️⃣ Clean Taxi Data

**Task:** `clean_taxi_data`  

**Handles missing values:**

- Numeric → median
- Categorical → mode
- Location IDs → 0

**Removes duplicates.**  

**Fixes outliers:**

- Trip distance: 0–100 miles
- Fare amount: 0–500
- Passenger count: 1–6

**Other corrections:**

- Corrects `total_amount` to match component sums
- Fixes pickup/dropoff datetime inconsistencies
- Removes invalid records (`trip_distance ≤0`, `fare ≤0`, missing datetime)

**Outcome:** Cleaned, reliable dataframe ready for transformation.

---

## 1️⃣2️⃣ Transform Taxi Data

**Task:** `transform_taxi_data`  

**Adds calculated fields:**

- Trip duration, average speed
- Pickup hour/day/month/year
- Time of day

**Flags:**

- Airport trip
- Credit card payment
- Weekend

**Other derived fields:**

- Revenue segment
- Tip percentage
- Valid trip boolean flag

**Outcome:** Transformed dataframe for analytics.

---

## 1️⃣3️⃣ Create Business Metrics

**Task:** `create_taxi_metrics`  

- Aggregates processed data into multiple business metrics:
  - Vendor performance metrics
  - Hourly demand
  - Pickup location analysis
  - Payment type analysis
  - Monthly summary  

**Outcome:** Dictionary of `metric_name -> pd.DataFrame`.

---

## 1️⃣4️⃣ Generate Processing Info

**Task:** `generate_processing_info_from_filenames`  

- Generates a `processing_id` (timestamped)  
- Extracts the date range from filenames for S3 partitioning

---

## 1️⃣5️⃣ Upload Data to S3 ✅

**Processed Data**

- Path: `processed/taxi/{processing_id}/combined.parquet`  
- Format: Parquet  
- Partitioning: `by date=%Y-%m` if `partitioning.by_date = true`  
- S3 Bucket: `taxiawsbucket`  
- Encryption: Enabled if `security.encryption_at_rest = true`  
- Versioning: Enabled if `security.versioning = true`  

**Business Metrics**

- Path: `processed/metrics/date={YYYY-MM}/metric_name.csv`  
- Format: CSV  
- Partitioning: `metrics_partitioning: date=*`  
- Bucket: `taxiawsbucket`  

**Metadata Updates**

- `processed/_metadata/processed_files.json`  
- `processed/_metadata/last_processed_watermark.txt`  

**Retention Policy**

- Raw: 365 days  
- Processed: 730 days  
- Metrics: 1095 days  
- Metadata: Keep indefinitely  

---

## 1️⃣6️⃣ Update Tracker & Watermark

**Tasks:**

- `mark_files_processed` → Adds the processed file keys to the tracker  
- `update_processing_watermark` → Updates watermark to the latest `LastModified` timestamp among processed files  

**Outcome:** Pipeline is ready for the next incremental run.

---

## Orchestration

**Prefect flows schedule and manage the workflow:**

- `extract_taxi_data` → Fetch raw files from S3  
- `deduplicate_files` → Remove duplicates, keep latest files  
- `process_taxi_data` → Transform & aggregate data  
- `load_processed_data` → Store results in S3 or local processed directories  

**Flow Features:**

- Incremental daily/hourly runs  
- Manual run option for on-demand execution  

---

## Tools & Technologies

- **Containerization:** Docker, Docker Compose  
- **Orchestration:** Prefect  
- **Storage:** AWS S3 (raw and processed buckets)  
- **Languages:** Python, SQL (optional for processing/validation)  
- **Logging & Monitoring:** Python logging + Prefect UI  
- **Testing:** Unit tests for transformation functions (Pytest)  

---

## Prefect Deployments

- **Daily incremental:** Processes only new data each day  
- **Hourly incremental:** Processes new data every hour  
- **Manual run:** Triggered on-demand to reprocess data  

**Prefect UI:** [http://prefect-server:4200](http://prefect-server:4200)

---

## How to Run

```bash
# Start Docker services
docker-compose up -d

# Run manual Prefect deployment
docker exec -it data-pipeline prefect deployment run 'incremental-taxi-data-processing/taxi-manual-run'
