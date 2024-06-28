#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(params):

    # extract the parameters
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url

    # make a case for when the downloaded data is a gzipped file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    # download the data from cloud and save into the output.csv file
    os.system(f"wget {url} -O {csv_name}")

    # connect to the database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # create a table in the db and insert all column names. if the table already exists, replace it.
    df = pd.read_csv(csv_name, nrows=5)
    df.head(n=0).to_sql(con=engine, name=table_name, if_exists='replace')

    # batch-load the data into the database in chunks of 100000 rows
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)


    while True:
        try:
            time_start = time()

            df = next(df_iter) # load the next chunk of data

            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)  # convert the dates columnn to datetime format
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

            df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append') # append the chunk to the existing table

            time_end = time()

            print("inserted another chunk... took %.3f seconds" % (time_end - time_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data into Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)


