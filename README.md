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

---

## Dataset
**Source:** AWS S3 (simulated or real taxi trip datasets)  

Variables extracted:  

- Trip ID  
- Pickup & Dropoff timestamps  
- Pickup & Dropoff locations  
- Fare amount  
- Trip distance  
- Passenger count  
- Vehicle type  

---

## Project Summary
The Taxi AWS Data Pipeline consists of:

### Extract
Python scripts fetch raw CSV/JSON files from AWS S3 and local directories.

### Load
Data is loaded into **raw/processed directories** for temporary storage before processing.  
Subsequent transformations update processed data to maintain **latest version of each file** (deduplication logic).

### Transform
Data transformations are applied using Python to clean, validate, and aggregate data.  
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

