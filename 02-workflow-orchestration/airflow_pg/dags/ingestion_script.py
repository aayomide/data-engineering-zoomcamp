import os

from time import time

import pandas as pd
from sqlalchemy import create_engine


def ingest_callable(user, password, host, port, db, table_name, csv_file, execution_date):
    print(table_name, csv_file, execution_date)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')  # connect to db
    engine.connect()                                                              # confirm the connection was successful

    print('connection to db established successfully, inserting data...')

    t_start = time()
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)           # load data in chunks

    df = next(df_iter)                                                         # load first chunk

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)          # convert date columns to datetime format
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(con=engine, name=table_name, if_exists='replace')      # insert table columns into db
    df.to_sql(con=engine, name=table_name, if_exists='append')                 # insert first chunk into db

    t_end = time()
    print('loaded and inserted the first chunk, took %.3f second' % (t_end - t_start))


    # load and insert next chunks into the db
    while True: 
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print("finished the ingestion.")
            break

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(con=engine, name=table_name, if_exists='append') 

        t_end = time()

        print('loaded and inserted another chunk, took %.3f second' % (t_end - t_start))


