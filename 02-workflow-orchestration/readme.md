## Module 2: Workflow Orchestration & Data Ingestion
### Learning Summary
- Set up an Airflow environment using docker-compose. A light-weight, less memory-intensive and resource-efficient Airflow configuration (single node setup) tailored for local environment was used
- Developed a solid grasp of Directed Acyclic Graphs (DAGs) in Airflow, including the various operators types such as action, transfer and sensor operators
- Explored the cost-benefit analysis of converting datasets from CSV to Parquet format for cloud storage, using `pyarrow`
- Modified the data ingestion script (for loading data to Postgres) from Week 1 to include parameters, allowing it to be used as functions within PythonOperator tasks in Airflow
- Orchestrated and scheduled two data pipelines for ingesting data into local Postgres database and Google Cloud Storage, respectively, using Airflow
    - Incrementally loaded the large CSV data into the local PostgreSQL database in small batches of a few thousand rows each
- Successfully ingested the NY Taxi Trips (2019–2020), Zone Lookup, and For-Hire Vehicle Trip datasets, as Parquet files into a GCS bucket
- Transferred datasets from AWS S3 to a Google Cloud Storage bucket using Google Transfer Service through both the console and Terraform

<br>

### Resources
- [Zoomcamp Airflow Notes: Setup & Data Ingestion](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/cohorts/2022/week_2_data_ingestion)
    - [Lightweight Airflow Setup with Docker](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/cohorts/2022/week_2_data_ingestion/airflow#setup---custom-no-frills-version-lightweight)
- Transfer Service - Moving Data from AWS to GCS: [Video](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/cohorts/2022/week_2_data_ingestion)