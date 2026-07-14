# NYC TLC BI Pipeline

## Introduction
This is an in-progress data analysis project, which takes all four datasets (*Yellow Taxis*, *Green Taxis*, *For Hire Vehicles* and *High-Volume For Hire Vehicles*) from the New York City Taxi and Limousine Commission, processes it and sends it to Power BI for analytics. Range of all datasets is from *January 2022* to *December 2023*.

### **Softwares**:
- *Programming Languages* : Python, SQL, PL/pgSQL
- *Databases* : PostgreSQL, Redis
- *Libraries* : PySpark, SQLAlchemy, Alembic
- *Version Control* : Git
- *BI Software* : Power BI Desktop
- *Orchestration* : Apache Airflow
- *Other Softwares* : Docker Desktop, Zed editor

### **Project Roadmap**:
- [x] Installation of all softwares using Docker
- [x] Ingestion of datasets from NYC TLC server (Data Extraction)
- [x] Cleaning and Enrichment of datasets using PySpark (Data Transformation)
- [x] Proper loading of datasets to PostgreSQL database (Data Loading)
- [x] Orchestrating all scripts to automate using Airflow (Data Orchestration)
- [x] Creation of aggregate table for HVFHV datasets using PL/pgSQL
- [x] Linking of PostgreSQL database with Power BI
- [x] Quarterly Analysis and Report Making using Power BI (Data Analysis)

> All scripts to be automated using Apache Airflow.
