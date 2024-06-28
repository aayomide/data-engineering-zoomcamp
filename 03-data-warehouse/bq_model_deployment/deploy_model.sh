
#Extract the BigQuery ML model from the BQ dataset it is located and save it to a GCS bucket: `
echo "Extracting the model from bq.."
bq.cmd --project_id de-zmcmp extract -m trips_data_all.tip_model gs://taxi_ml_model_dzc/tip_model`

#Create a temporary directory to store the extracted model files locally
mkdir /tmp/model

#Copy the model from Google Cloud Storage to the local directory: `
echo "Copying model from GCS to temp folder.."
gsutil cp -r gs://taxi_ml_model_dzc/tip_model /tmp/model

#Create the serving directory structure
mkdir -p serving_dir/tip_model/1

#Copy the model files to the serving directory
echo "Copying model from temp to serving dir.."
cp -r /tmp/model/tip_model/* serving_dir/tip_model/1
# rm -rf /tmp/model

#Pull the TensorFlow Serving Docker image
echo "Pulling tf serving image.."
docker pull tensorflow/serving

# #Run TensorFlow Serving with the model
# echo "Running tf serving image.."
# docker run -p 8501:8501 --mount type=bind,source=$(pwd)/serving_dir/tip_model,target=/models/tip_model -e MODEL_NAME=tip_model -t tensorflow/serving

# #Make a prediction using the served model
# curl -d '{"instances": [{"passenger_count":1, "trip_distance":11.1, "PULocationID":"195", "DOLocationID":"264", "payment_type":"2","fare_amount":34.5,"tolls_amount":0.0}]}' -X POST http://localhost:8501/v1/models/tip_model:predict