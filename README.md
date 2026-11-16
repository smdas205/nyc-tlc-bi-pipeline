# NYC TLC BI Pipeline

## Introduction
This is an in-progress data analysis project, which takes all four datasets (*Yellow Taxis*, *Green Taxis*, *For Hire Vehicles* and *High-Volume For Hire Vehicles*) from the New York City Taxi and Limousine Commission, processes it and sends it to Power BI for analytics. Range of all datasets is from *January 2022* to *December 2023*.

> PLEASE NOTE:
> 
> *MAJOR CHANGE IN FUTURE*
> 
> The whole project is being shifted to a cloud based project. This is to primarily save storage in my laptop, since the files generated during the whole pipeline are huge and I have very
> limited storage in my laptop. Tech stack and project process is going to have significant changes owing to the change to cloud based project.

### **Softwares**:
- *Programming Languages* : Python, SQL, PL/pgSQL
- *Databases* : PostgreSQL, Redis
- *Libraries* : PySpark, SQLAlchemy, Alembic
- *Version Control* : Git
- *BI Software* : Power BI Desktop
- *Orchestration* : Apache Airflow
- *Other Softwares* : Docker Desktop, Zed editor

## **Process**:

### **1. Environment Setup:**
- Containerized the full stack with Docker Desktop: PostgreSQL, Redis, Apache Spark and Apache Airflow v3.2.1 using Docker Compose file. Kept Python versions consistent across containers to v3.12.
- Created the file structure for the project, focusing on better integration with Docker containers for less space overhead.
    
### **2. Data Extraction (Ingestion):**
- Built ingestion scripts to pull Yellow Taxi, Green Taxi, For-Hire Vehicles (FHV) and High Volume For-Hire Vehicles (HVFHV) datasets directly from the NYC TLC server. Scripts are able to handle extraction of any date, limited the project scope from January 2022 to December 2023 to minimize storage overhead.
- Used a Redis hash to track download status and metadata per file, allowing for safe reruns using Airflow without downloading previous files repeatedly.
    
### **3. Data Transformation (Cleaning and Enrichment):**
- Wrote a PySpark pipeline consisting of the following steps: Clean -> Enrich -> Validate. Converted all files to have a canonical schema for standardization across all four datasets.
- Enriched trips with derived time metrics, fare metrics and rate metrics.
- Used Pyspark native validation functions instead of Great Expectations because of incompatibility with Python 3.12.
- Resolved Spark Executor OOM (Out Of Memory) kills by raising Spark Worker memory to 6 GB.
    
### **4. Data Loading:**
- Designed a Star Schema with three fact tables: 
  1. *taxi_trips* to store both Yellow and Green Taxi trips using an additional taxi_type column.
  2. *fhv_trips* to store For Hire Vehicles (FHV) trips.
  3. *high_volume_fhv_trips* to store High Volume FHV trips.
- To support the fact tables, added seven dimension tables as follows:
  1. *vendor_table* to store TPEP Vendor information for both Yellow and Green taxis.
  2. *trip_type_table* to specify Street-Hail/Dispatch for Green Taxi trips.
  3. *rate_code_table* specifies the Rate Code for Yellow and Green Taxi trips.
  4. *payment_table* specifies the Payment Type for Yellow and Green Taxi trips.
  5. *location_based_attributes* stores Location Borough and Zone for all taxi trips.
  6. *hvfhs_table* specifies taxi aggregator codes for HVFHV trips.
  7. *base_table* specifies FHV base information for HVFHV and FHV trips.
- Integrated the PostgreSQL JDBC driver for Spark writes, tuned with 'fetchsize=10000' to keep memory overhead low.

### **5. Orchestration:**
- Built DAG scripts for ingestion, transformation and loading using Python and Airflow.
- Using `TriggerDagRunOperator` in Airflow, built a DAG master orchestration script, creating an ordered pipeline that executes DAG scripts as ingestion > transformation > loading.
- Kept a dedicated heavy_queue in Celery for larger HVFHV workloads.
- Debugged JWT/Fernet key mismatches, an `UnboundLocalError` in `TriggerDagRunOperator` with `deferrable=True`, and duplicate `run_id` constraint violations along the way.
    
### **6. Aggregation:**
- To lighten load on system and improve Power BI Desktop response, built three aggregate tables for High Volume FHV trips:
  1. 'hvfhv_location_summary': Stores pickup and location trip counts per day.
  2. 'hvfhv_daily_summary':  Stores total trip count and total revenue per day.
  3. 'hvfhv_service_summary': Stores HVFHS provider total driver pay and platform take per day.
- Truncated 'high_volume_fhv_trips' after populating aggregate tables to reclaim space from Docker.

### **7. Data Quality Validation:**
- Removed close to 25,000 rows which represented implausible trips and false data, using SQL filters for speed threshold of 65 mph (interstate speed limit in NY State) and implausible speed/distance/fare combinations.
- Identified and documented a systemic rate code 4 (Nassau/Westchester) misclassification affecting airport trips, and built targeted validation queries for fare/rate code anomalies.
    
### **8. Power BI Linking:**
- Connected all tables to Power BI Desktop (except location_based_attributes) using Import mode; *high_volume_fhv_trips* was not connected since it was truncated. 
- Created two Power BI specific tables: Pickup Location Table and Drop-Off Location Table, both created from location_based_attributes (which was connected through DirectQuery mode) to keep both Pickup and Drop-Off relationship across fact and aggregate tables active.
- Reprojected an NYC shapefile from EPSG:2263 to WGS84 and saved it as TopoJSON file using [MapShaper](https://mapshaper.org/) for Power BI Desktop to recognise it as Shape Map.
    
### **9. Analysis and Reporting:**
- Built an 8 page Power BI report covering quarterly reports across all taxi trips.
- Used Shape Map visual to visualize Pickup and Drop-Off counts across all taxi trips, with added slicers for month, taxi type, borough and zone for granular analysis.
- Used a scatter chart to visualize distance v/s base fare for Yellow and Green Taxi trips, using Rate Code as additional analysis for fare trends.
- Using quarter analysis and reviewing all 8 reports, was able to spot a notable trend in taxi rides: Staten Island taxi trips drop by a significant amount in Q4 2023, owing to implementation of two-way tolls across the Verrazzano-Narrows bridge for taxis.

> Up next for this project: Analysing quarterly reports and adding more data quality checks to Spark scripts.

> Up next for me: Implementing a data streaming pipeline using both cloud and offline softwares.
