from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color:str, year:int, month:int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gsc")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return f"./{gcs_path}"

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passengers: {df.passenger_count.isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passengers: {df.passenger_count.isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="dtc-de-373401",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq(month: int, year: int, color: str):
    """Main ETL flow to load data into BigQuery"""
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    return len(df)

@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    total_rows = 0
    for month in months:
        rows = etl_gcs_to_bq(month, year, color)
        total_rows += rows
    print("total rows: ", total_rows)


if __name__ == "__main__":
    color = "yellow"
    year = 2019
    months = [2, 3]
    etl_parent_flow(months, year, color)
