import os
from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from google.cloud import storage


@task(retries=3)
def fetch(url: str) -> pd.DataFrame:
    """Fetch data from the web into pandas DataFrame"""
    df = pd.read_csv(url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the data"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
    df['tpep_dropoff_datetime'] = pd.to_datetime(df.tpep_dropoff_datetime)
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFame out locally as parquet file"""
    os.makedirs(f"data/{color}", exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression='gzip')
    return path

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
def etl_web_to_gcp() -> None:
    """The main ETL function"""
    color = "yellow"
    year = 2019
    month = 3
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcp()
