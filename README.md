# Taxi AWS Data Pipeline

## Architecture
![Architecture Diagram](link-to-your-architecture-image)  
*Illustration showing Prefect orchestration, Dockerized services, S3 storage, and batch/incremental flows.*

---

## Motivation
The goal of this project is to gain hands-on experience with modern **data engineering tools** like Python, Docker, Prefect, and AWS S3 by building a fully functional **incremental taxi data pipeline**.

To make the pipeline robust and production-ready:

- **Data deduplication and batch processing** implemented in Python  
- **Orchestration** is done using Prefect flows  
- **Containerized** with Docker and managed via Docker Compose  
- **Incremental and manual flow runs** supported for flexibility  
- Logging implemented for monitoring pipeline health and process status
- **Watermarking** used to avoid re-processing old data

---

## Dataset
**Source:** NYC Taxi and Limousine Commission: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Variables extracted:  

- VendorID: A code indicating the TPEP provider that provided the record. 1 = Creative Mobile Technologies, LLC, 2 = Curb Mobility, LLC, 6 = Myle Technologies Inc, 7 = Helix
- tpep_pickup_datetime: The date and time when the meter was engaged.
- tpep_dropoff_datetime:The date and time when the meter was disengaged. 
- passenger_count: The number of passengers in the vehicle. 
- trip_distance: The elapsed trip distance in miles reported by the taximeter.
- RatecodeID: The final rate code in effect at the end of the trip. 1 = Standard rate, 2 = JFK, 3 = Newark, 4 = Nassau or Westchester, 5 = Negotiated fare, 6 = Group ride, 99 = Null/unknown.
- store_and_fwd_flag: This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka “store and forward,” because the vehicle did not have a           connection to the server. Y = store and forward trip, N = not a store and forward trip.
- PULocationID: TLC Taxi Zone in which the taximeter was engaged.
- DOLocationID: TLC Taxi Zone in which the taximeter was disengaged.
- payment_type: A numeric code signifying how the passenger paid for the trip. 0 = Flex Fare trip, 1 = Credit card, 2 = Cash, 3 = No charge, 4 = Dispute, 5 = Unknown, 6 = Voided trip
- fare_amount: The time-and-distance fare calculated by the meter.
- extra: Miscellaneous extras and surcharges.
- mta_tax: Tax that is automatically triggered based on the metered rate in use.
- tip_amount: Tip amount – This field is automatically populated for credit card tips. Cash tips are not included.
- tolls_amount: Total amount of all tolls paid in trip.
- improvement_surcharge: Improvement surcharge assessed trips at the flag drop. The improvement surcharge began being levied in 2015.
- total_amount: The total amount charged to passengers. Does not include cash tips.
- congestion_surcharge: Total amount collected in trip for NYS congestion surcharge.
- airport_fee: For pick up only at LaGuardia and John F. Kennedy Airports.


---

## Project Summary
The Taxi AWS Data Pipeline consists of:

### Extract
Python scripts fetch raw CSV/JSON files from AWS S3 and local directories.

### Load
Data is loaded into **raw directories** for temporary storage before processing.  
Subsequent transformations update processed data to maintain **latest version of each file** (deduplication logic).

### Transform
Applied consistent Schema for all files

**Clean taxi data:**

- **Missing Values:**
  - Numeric → median
  - Categorical → mode
  - Location IDs → 0

- **Fixed Outliers:**
  - Trip distance: 0–100 miles
  - Fare amount: 0–500
  - Passenger count: 1–6

- **Corrected total_amount** to match component sums.

- **Fixed pickup/dropoff datetime** inconsistencies.

- **Removed invalid records:**
  - `trip_distance ≤ 0`
  - `fare ≤ 0`
  - Missing datetime

### Transform taxi data

**Adds calculated fields:**

- Trip duration
- Average speed
- Pickup hour / day / month / year
- Time of day

**Flags:**

- Airport trip
- Credit card payment
- Weekend

**Other derived fields:**

- Revenue segment
- Tip percentage
- Valid trip boolean flag


Metrics Generated: Vendor Performance Metrics, Hourly Demand Patterns, Location Analysis, Payment Type Analysis, Monthly Summary.
Batch processing ensures optimized handling for large datasets.

### Orchestration
**Prefect flows** schedule and manage the workflow:  

1. `extract_taxi_data` → Fetch raw files from S3  
2. `deduplicate_files` → Remove duplicates, keep latest files  
3. `process_taxi_data` → Transform & aggregate data  
4. `load_processed_data` → Store results in S3 or local processed directories  

Flows include **incremental daily/hourly runs** and a **manual run** option for on-demand execution.  

### Visualization
Processed data can be analyzed with **Power BI or Jupyter Notebooks** to create KPIs like:  

- Daily trip counts  
- Revenue per day/hour  
- Average trip distance  
- Vehicle type distribution  

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

Prefect UI: [http://prefect-server:4200](http://prefect-server:4200)

---

## How to Run
```bash
# Start Docker services
docker-compose up -d

# Run manual Prefect deployment
docker exec -it data-pipeline prefect deployment run 'incremental-taxi-data-processing/taxi-manual-run'

