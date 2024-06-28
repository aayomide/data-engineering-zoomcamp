-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;


/*
-- Creating external table referring to gcs path; here we combine all monthly yellow taxi data from 2019 & 2020
CREATE OR REPLACE EXTERNAL TABLE trips_data_all.external_yellow_tripdata_2020_2021_2019_2020
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc-tl-data/trip data/yellow_tripdata_2019-*.csv', 'gs://nyc-tl-data/trip data/yellow_tripdata_2020-*.csv']
); 
*/


CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.external_yellow_tripdata_2020_2021`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyrides_dezmcmp_datalake/raw/yellow_tripdata_2020-*.parquet', 'gs://nyrides_dezmcmp_datalake/raw/yellow_tripdata_2021-*.parquet']
);


-- Check yellow trip data
SELECT * FROM trips_data_all.external_yellow_tripdata_2020_2021 limit 10;
SELECT COUNT(*) FROM trips_data_all.external_yellow_tripdata_2020_2021;


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE trips_data_all.yellow_tripdata_non_partitoned AS
SELECT * FROM trips_data_all.external_yellow_tripdata_2020_2021;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE trips_data_all.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM trips_data_all.external_yellow_tripdata_2020_2021;

-- Impact of partition
-- Scanning 146MB of data
SELECT DISTINCT(VendorID)
FROM trips_data_all.yellow_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2020-12-01' AND '2020-12-31';

-- Scanning ~22MB of DATA
SELECT DISTINCT(VendorID)
FROM trips_data_all.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2020-12-01' AND '2020-12-31';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `trips_data_all.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE trips_data_all.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM trips_data_all.external_yellow_tripdata_2020_2021;

-- Query scans 22 MB
SELECT count(*) as trips
FROM trips_data_all.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2020-12-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 21 MB
SELECT count(*) as trips
FROM trips_data_all.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2020-12-01' AND '2020-12-31'
  AND VendorID=1;
  
