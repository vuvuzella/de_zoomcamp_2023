from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials
from datetime import timedelta
from typing import Tuple
import requests
import os
import pandas as pd


@task(log_prints=True, retries=3, retry_delay_seconds=3, retry_jitter_factor=1.5)
def extract_data(url_source: str, dest_folder: str) -> str:
    """
    Extract data from web source
    write it as parquet file in data source
    """
    response = requests.get(url_source)
    print(url_source)
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    filename = url_source.split("/")[-1]
    csv_fp = f"{dest_folder}/{filename}"
    open(csv_fp, "wb").write(response.content)
    return csv_fp


@task(log_prints=True, retries=3, retry_delay_seconds=3, retry_jitter_factor=1.5)
def write_parquet(data_fp: str):
    """
    Convert csv to parquet file
    """
    df = pd.read_csv(data_fp)
    parquet_fp = data_fp.replace(".csv.gz", ".parquet")
    df.to_parquet(path=parquet_fp, compression="gzip")
    return parquet_fp


@task(log_prints=True, retries=3, retry_delay_seconds=3, retry_jitter_factor=1.5)
def transform_taxi_data(data_fp: str) -> None:
    """
    Make columns in lowercase
    """
    df = pd.read_csv(data_fp, index_col=False)
    print(df.columns)
    df.columns = [str(column).lower() for column in df.columns]
    df.to_csv(data_fp, compression="gzip", index=False)


@task(log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(1))
def transform_fhv_data(data_fp: str):
    ...


@task(log_prints=True, retries=3, retry_delay_seconds=3, retry_jitter_factor=1.5)
def upload_to_bigquery(data_fp: str, destination_table: str):
    """
    Upload the data to a dataset in bigquery
    """
    gcp_credential_block = GcpCredentials.load("service-account-cred")
    df = pd.read_parquet(data_fp)
    df.to_gbq(
        destination_table,
        project_id="de-zoomcamp-376020",
        chunksize=1000,
        location="australia-southeast1",
        progress_bar=True,
        credentials=gcp_credential_block.get_credentials_from_service_account(),  # type: ignore
        if_exists="append",
    )


# TODO: add task to upload to postgresql
@task
def upload_to_postgresq(data_fp: str, db: str):
    """
    Read Parquet File
    Upload to postgresql database
    """
    ...


# separate taxi data flow and tasks into its own file
@flow(name="Week 4 ETL - Taxi Trip Data", log_prints=True)
def taxi_data_etl(
    color: str, years: list[int], months: list[int], dest_dataset: str, dest_table: str
):
    base_link = (
        f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}"
    )
    data_source = [
        f"{base_link}/{color}_tripdata_{year}-{month:02}.csv.gz"
        for year in years
        for month in months
    ]
    for source in data_source:
        csv_fp = extract_data(source, f"data/taxi_data/{color}")
        transform_taxi_data(csv_fp)
        parquet_fp = write_parquet(csv_fp)
        upload_to_bigquery(parquet_fp, f"{dest_dataset}.{dest_table}")


# TODO: Add fhv etl pipeline
@flow(name="Week 4 ETL - For Hire Vehicle Data", log_prints=True)
def fhv_data_etl(data_source_list: list[str]):
    for source in data_source_list:
        parquet_data_fp = extract_data(source, "")
        # transform_fhv_data(parquet_data_fp)
        # upload_to_bigquery(
        #     parquet_data_fp,
        # )


def main():
    dest_dataset = "week_4_dbt"
    taxi_data_etl(
        color="yellow",
        years=[2019, 2020],
        months=list(range(1, 13)),
        dest_dataset=dest_dataset,
        dest_table="yellow_taxi_data",
    )
    taxi_data_etl(
        color="green",
        years=[2019, 2020],
        months=list(range(1, 13)),
        dest_dataset=dest_dataset,
        dest_table="green_taxi_data",
    )


if __name__ == "__main__":
    main()
