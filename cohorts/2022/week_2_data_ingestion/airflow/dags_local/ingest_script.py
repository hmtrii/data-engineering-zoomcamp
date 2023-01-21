import time

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine


def ingest_callable(user, password, host, port, db, table_name, parquet_file):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    parquet_file = pq.ParquetFile(parquet_file)

    for batch in parquet_file.iter_batches(batch_size=100000):
        start = time.time()
        df = batch.to_pandas()
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='replace')
        end = time.time()
        print('inserted another chunk, took %.3f second' % (end - start))

    print('Comppleted ingest data')