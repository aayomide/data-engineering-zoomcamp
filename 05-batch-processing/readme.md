## Module 5: Batch Processing (PySpark)

### Learning Summary
- Spark and PySpark Setup on Linux: Installed and configured Spark and PySpark in a Linux environment using detailed guides and example code
- Distributed Processing in Spark: Loaded and partitioned large file inputs for distributed processing in Spark
- Spark Cluster Architecture: Acquired comprehensive knowledge of the Spark Cluster architecture, where the master node receives Spark submit requests and distributes tasks among the Spark executors.
- Resilient Distributed Datasets (RDD): Developed proficiency with RDD, the foundational data structure for Spark DataFrames. And learned how to express SQL statements in RDD terms
- Running Spark in the Cloud
    - Created a local Spark cluster in a VM:
        - Connected Spark jobs to Google Cloud Storage (GCS)
        - Converted notebooks into scripts using `jupyter nbconvert --to=script <notebook_path>`
        - Utilized spark-submit to submit Spark jobs (scripts) to a Spark cluster
        - Parameterized the Spark scripts to run for different months/years
    - Created a Spark Cluster in a Google-managed service
        - Set up a Dataproc Cluster and ran Spark jobs with Dataproc. Dataproc lets us provision Apache Hadoop clusters and connect to underlying analytic data stores.
        - Submitted Spark jobs to a DataProc cluster via the Web UI and via the Cloud SDK (using `gcloud` command)
- Connecting Spark to BigQuery: Configured Spark jobs to write results directly to a data warehouse (BigQuery) instead of back to the data lake, as was done previously

### Resources
- [Zoomcamp Batch Processing Videos & Notes](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/05-batch)
- [Zoomcamp - Guide to Install Spark and Pyspark](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/linux.md)
    - [A Step-by-Step Guide to Install PySpark on Linux with Example Code](https://www.machinelearningplus.com/pyspark/install-pyspark-on-linux/)
- [Configuring a Dataproc Cluster to Run Spark Jobs](https://youtu.be/osAiAYahvh8)
- [Connecting Spark to BQ](https://youtu.be/HIm2BOj8C0Q)