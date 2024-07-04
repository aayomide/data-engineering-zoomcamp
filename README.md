# DATA & ANALYTICAL ENGINEERING


## Overview

### Architecture diagram
<img src="https://images.datacamp.com/image/upload/v1691745028/image1_3ea61fa567.png" style="width: 600px; height: auto;"/>


### Technologies
* Google Cloud Platform (GCP)
  * Google Cloud Storage (GCS): Data Lake
  * BigQuery: Data Warehouse
  * Compute Engine: Virtual Machine
* Terraform: Infrastructure-as-Code (IaC)
* Docker: Containerization
* SQL: Data Analysis & Exploration
* Airflow: Pipeline Orchestration
* DBT: Data Transformation
* Spark: Distributed Processing
* Kafka: Streaming

## Syllabus

### [Module 1: Tools & Environment Setup](/01-docker-terraform/)

* Introduction to GCP
* Docker and docker-compose 
* Running Postgres locally with Docker
* Setting up infrastructure on GCP with Terraform
* Preparing the environment for the course


### [Module 2: Data ingestion](/02-workflow-orchestration/02)

* Introduction to Data Lake (GCS) 
* Introduction to Orchestration (Airflow) 
  * Setting up Airflow with Docker: 
  * Data ingestion DAG: 
    * Cloud-based, with GCP (GCS + BigQuery)
    * Local, with Postgres
* Transfer Service (AWS -> GCP)



### [Module 3: Data Warehouse](/03-data-warehouse/)
* Introduction to BigQuery & BQ Internals
* Cost-Benefits of using BigQuery
* Data Partitioning and Clustering, Automatic re-clustering 
* Pointing to a location in Google Storage
* Loading data to BigQuery & Postgres
* BigQeury best practices
* BigQuery Geo location, BigQuery ML 
* Alternatives (Snowflake/Redshift)



### [Module 4: Analytics engineering](/04-analytical-engineering/)
* Basics 
    * What is DBT?
    * ETL vs ELT 
    * Data modeling
    * DBT fit of the tool in the tech stack
* Usage (Combination of coding + theory)
    * Anatomy of a dbt model: written code vs compiled Sources
    * Materialisations: table, view, incremental, ephemeral  
    * Seeds 
    * Sources and ref  
    * Jinja and Macros 
    * Tests  
    * Documentation 
    * Packages 
    * Deployment: local development vs production 
    * DBT cloud: scheduler, sources, and data catalog (Airflow)
    * DBT cli (local)
* Google Looker Studio: Dashboard



### [Module 5: Batch processing](/05-batch-processing)

* Distributed processing (Spark) 
    * Explaining Spark and Spark clusters
    * Use Cases and Alternatives (Flink, Hive, Presto, etc.)
    * Broadcast variables, Partitioning, Shuffle
    * RDDs, SparkSQL
    * Spark in the Cloud with Google Cloud Dataproc
* Extending Orchestration env (airflow)
    * BigQuery on Airflow
    * Spark on Airflow 



### [Module 6: Streaming](06-stream-processing)

* Basics
    * Kafka and Internals of Kafka, broker
    * Partitoning of Kafka topic
    * Replication of Kafka topic
* Consumer-producer
* Schemas (Avro)
* Streaming: Kafka streams
* Kafka Connect
* Alternatives (PubSub/Pulsar)



### [Project](https://github.com/aayomide/crypto_analytics_engineering/)
