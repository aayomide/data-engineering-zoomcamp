## Running Spark in the Cloud

### Connecting to Google Cloud Storage 
*Before running the following commands, be sure to have authenticated with gcloud using `gcloud auth login`*

Uploading data to GCS:

```bash
gsutil -m cp -r data/pq/ gs://data_lake_de-zmcmp-spark-nytaxi/pq
```

Download the jar for connecting to GCS to any location (e.g. the `lib` folder):

First create a folder named `lib` in the home directory and cd into it
```bash
mkdir lib
cd lib/
```

then download the Cloud Storage Connector for Hadoop to enable PySpark connect to GCS.

```bash
gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar gcs-connector-hadoop3-2.2.5.jar
```

See the notebook with configuration in [07_spark_gcs.ipynb](09_spark_gcs.ipynb)

(Thanks Alvin Do for the instructions!)


### Local Cluster and Spark-Submit

Creating a stand-alone cluster ([docs](https://spark.apache.org/docs/latest/spark-standalone.html)):

```bash
./sbin/start-master.sh
```

Creating a worker:

```bash
URL="spark://de-zoomcamp.europe-west1-b.c.de-zoomcamp-nytaxi.internal:7077"
./sbin/start-slave.sh ${URL}

# for newer versions of spark use that:
#./sbin/start-worker.sh ${URL}
```

Turn the notebook into a script:

```bash
jupyter nbconvert --to=script 04_spark_sql.ipynb
```

Edit the script and then run it:

```bash 
python 04_spark_sql.py \
    --input_green=data/pq/green/2020/*/ \
    --input_yellow=data/pq/yellow/2020/*/ \
    --output=data/report/report-2020
```

Use `spark-submit` for running the script on the cluster

```bash
URL="spark://de-zoomcamp.europe-west1-b.c.de-zoomcamp-nytaxi.internal:7077"

spark-submit \
    --master="${URL}" \
    04_spark_sql.py \
        --input_green=data/pq/green/2021/*/ \
        --input_yellow=data/pq/yellow/2021/*/ \
        --output=data/report/report-2021
```

### Google Cloud Dataproc

Upload the script to GCS:

```bash
gsutil cp 04_spark_sql.py gs://<gcs-bucket-name>/code/04_spark_sql.py
```

Params for the job:

* `--input_green=gs://data_lake_de-zmcmp-spark-nytaxi/pq/green/2021/*/`
* `--input_yellow=gs://data_lake_de-zmcmp-spark-nytaxi/pq/yellow/2021/*/`
* `--output=gs://data_lake_de-zmcmp-spark-nytaxi/report/report-2021`


Using Google Cloud SDK for submitting to dataproc
([link](https://cloud.google.com/dataproc/docs/guides/submit-job#dataproc-submit-job-gcloud))

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=dtc-zmcmp-cluster \
    --region=europe-west6 \
    gs://data_lake_de-zmcmp-spark-nytaxi/code/04_spark_sql.py \
    -- \
        --input_green=gs://data_lake_de-zmcmp-spark-nytaxi/pq/green/2020/*/ \
        --input_yellow=gs://data_lake_de-zmcmp-spark-nytaxi/pq/yellow/2020/*/ \
        --output=gs://data_lake_de-zmcmp-spark-nytaxi/report/report-2020
```

### Connecting to BigQuery

Upload the script to GCS:

```bash
gsutil cp 04_spark_sql_big_query.py gs://<gcs-bucket-name>/code/04_spark_sql_big_query.py
```

Write results to big query ([docs](https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#pyspark)):

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=dtc-zmcmp-cluster \
    --region=europe-west6 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://data_lake_de-zmcmp-spark-nytaxi/code/04_spark_sql_big_query.py \
    -- \
        --input_green=gs://data_lake_de-zmcmp-spark-nytaxi/pq/green/2020/*/ \
        --input_yellow=gs://data_lake_de-zmcmp-spark-nytaxi/pq/yellow/2020/*/ \
        --output=trips_data_all.reports-2020
```

