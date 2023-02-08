import os
from pathlib import Path
from datetime import timedelta
import requests

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash

from google.cloud import storage


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download(dataset_url: str, dataset_file) -> pd.DataFrame:
    os.makedirs(f"data/fhv", exist_ok=True)
    path = Path(f"data/fhv/{dataset_file}.csv.gz")
    with open(path, "wb") as f:
        f.write(requests.get(dataset_url).content)
    return path
    

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the data"""
    try:
        df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
        df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)
    except AttributeError:
        df['lpep_pickup_datetime'] = pd.to_datetime(df.lpep_pickup_datetime)
        df['lpep_dropoff_datetime'] = pd.to_datetime(df.lpep_dropoff_datetime)
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task()
def write_gcs(path: Path) -> None:
    """Upload the parquet file to GCS"""

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    gcs_block = GcsBucket.load("zoom-gsc")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )

@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

    path = download(dataset_url, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021
):
    for month in months:
        etl_web_to_gcs(year, month)

if __name__ == "__main__":
    months = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    year = 2019
    etl_parent_flow(months, year)
