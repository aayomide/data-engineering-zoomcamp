## Model deployment
[Tutorial](https://cloud.google.com/bigquery-ml/docs/export-model-tutorial)

### Steps
- Authenticate with Google Cloud: `gcloud auth login`
- Extract the BigQuery ML model from the BQ dataset it is located and save it to a GCS bucket: `bq --project_id de-zmcmp extract -m trips_data_all.tip_model gs://taxi_ml_model_dzc/tip_model`
  - If `bq` command doesn't work and you see an error like "bash: bq: command not found", you may need to try `bq.cmd` and ensure that Google Cloud SDK is added to your system's PATH.
- Create a temporary directory to store the extracted model files locally: `mkdir /tmp/model`
- Copy the model from Google Cloud Storage to the local directory: `gsutil cp -r gs://taxi_ml_model_dzc/tip_model /tmp/model`
- Create the serving directory structure: `mkdir -p serving_dir/tip_model/1`
- Copy the model files to the serving directory: `cp -r /tmp/model/tip_model/* serving_dir/tip_model/1`
- Pull the TensorFlow (TF) Serving Docker image: `docker pull tensorflow/serving`
- Run TF Serving with the model: `docker run -p 8501:8501 --mount type=bind,source=$(pwd)/serving_dir/tip_model,target=/models/tip_model -e MODEL_NAME=tip_model -t tensorflow/serving`
  - this runs the TensorFlow Serving Docker container
  - binds the local directory serving_dir/tip_model to the container's /models/tip_model directory
  - sets the MODEL_NAME environment variable to tip_model
  - the container is run in the background (&) and serves the model on port 8501
- Make a prediction using the served model: `curl -d '{"instances": [{"passenger_count":1, "trip_distance":11.1, "PULocationID":"195", "DOLocationID":"264", "payment_type":"2","fare_amount":34.5,"tolls_amount":0.0}]}' -X POST http://localhost:8501/v1/models/tip_model:predict`
  - this uses curl to send a POST request to the TensorFlow Serving endpoint to make a prediction
  - the request payload contains the input features for the model: passenger_count, trip_distance, PULocationID, DOLocationID, payment_type, fare_amount, and tolls_amount
  - the response will contain the predicted tip amount.
  - As opposed to this, you can also use Postman to test the API

>NB: You can use `bash deploy_model.sh` to run the first few steps. Then, jump to the "Run the TF serving.." step and make a prediction aftewards by running the curl command above

  http://localhost:8501/v1/models/tip_model