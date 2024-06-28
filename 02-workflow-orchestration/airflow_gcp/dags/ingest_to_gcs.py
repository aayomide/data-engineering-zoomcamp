import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq


# environmental variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

dataset_filename =  "green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"
dataset_zipfilename =  "green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz"
dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/{dataset_zipfilename}"
parquet_filename = dataset_filename.replace('.csv', '.parquet')


def format_to_parquet(src_file):
    print(src_file, os.path.getsize(src_file))  # prince file name and size
    if not (src_file.endswith('.csv') or src_file.endswith('.csv.gz')):
        logging.error("Can only accept source files in .CSV or CSV.gz format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv.gz', '.parquet'))


#NB: it takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Uploaded the local files to GCS

    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74S)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    # create a client for gcs
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file, timeout=300)


default_args = {
    "owner": "airflow",
    # "start_date": days_ago(1),
    "start_date": datetime(2020, 10, 1),  # note: airflow defaults timezone is UTC
    "end_date": datetime(2021, 3, 1),
    # "depends_on_past": False,
    "retries": 2,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=2,
    tags=['de-zmcmp-afw'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        # bash_command=f"curl -sSL {dataset_url} | head -n 10 > {path_to_local_home}/{dataset_filename}.gz && ls {path_to_local_home}"
        # bash_command = f'curl -sSL {dataset_url} | head -n 10 > {path_to_local_home}/{dataset_zipfilename} \
        #                 && zcat {path_to_local_home}/{dataset_zipfilename} | head -n 20 > {path_to_local_home}/{dataset_filename} | xargs rm -f {path_to_local_home}/{dataset_zipfilename} \
        #                 && ls {path_to_local_home} && more {path_to_local_home}/{dataset_filename}'
        bash_command = f'curl -sSL {dataset_url} > {path_to_local_home}/{dataset_zipfilename} && ls {path_to_local_home}'
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_zipfilename}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_filename}",
            "local_file": f"{path_to_local_home}/{parquet_filename}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_filename}"],
            },
        },
    ),

    remove_files_task = BashOperator(
        task_id="remove_files_task",
        bash_command = f'ls {path_to_local_home} \
                        && rm -f {path_to_local_home}/{dataset_zipfilename} {path_to_local_home}/{dataset_filename} {path_to_local_home}/{parquet_filename} \
                        && ls {path_to_local_home}'
    )


    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task >> remove_files_task