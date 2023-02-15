from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from typing import Tuple
import requests
import os
import pandas as pd


@task(log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(1))
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
    print(csv_fp)
    open(csv_fp, "wb").write(response.content)
    return csv_fp


@task(log_prints=True)
def write_parquet(data_fp: str):
    df = pd.read_csv(data_fp)
    print(df.columns)
    parquet_fp = data_fp.replace(".csv.gz", ".parquet")
    print(parquet_fp)
    df.to_parquet(path=parquet_fp, compression="gzip")
    return parquet_fp


@task
def transform_taxi_data(data_fp: str) -> None:
    """
    Read Parquet file
    Normalize columns
    save the changes to Parquet file
    """
    df = pd.read_csv(data_fp)
    df.columns = [column.lower() for column in df.columns]  # type: ignore
    df.to_csv(data_fp, compression="gzip")


@task
def transform_fhv_data(data_fp: str):
    ...


@task
def upload_to_bigquery(data_fp: str):
    """
    Read Parquet file
    Upload the data to a dataset in bigquery
    """


@flow(name="Week 4 ETL - Taxi Trip Data", log_prints=True)
def taxi_data_etl(data_source_list: list[str]):
    for source in data_source_list:
        csv_fp = extract_data(source, "data/taxi_data")
        transform_taxi_data(csv_fp)
        parquet_fp = write_parquet(csv_fp)
        # transform_taxi_data(parquet_data_fp)
        # upload_to_bigquery(
        #     parquet_data_fp,
        # )


@flow(name="Week 4 ETL - For Hire Vehicle Data", log_prints=True)
def fhv_data_etl(data_source_list: list[str]):
    for source in data_source_list:
        parquet_data_fp = extract_data(source, "")
        # transform_fhv_data(parquet_data_fp)
        # upload_to_bigquery(
        #     parquet_data_fp,
        # )


def main():
    base_link = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    taxi_data_etl(
        [
            f"{base_link}/yellow_tripdata_2019-{num:02}.csv.gz"
            for num in list(range(1, 2))
        ],
    )


if __name__ == "__main__":
    main()
