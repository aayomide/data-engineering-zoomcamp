Quick hack to load files directly to GCS, without Airflow*. It downloads csv files from [here](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/) and uploads them to your Cloud Storage Bucket as parquet files.

1. Install pre-reqs (more info in `web_to_gcs.py` script)
2. Run: `python web_to_gcs.py`
