from datetime import timedelta
from prefect import task
from prefect.tasks import task_input_hash
from pipelines import Pipeline

import pandas as pd
from pandas import DataFrame, read_csv
from pandas.io.parsers import TextFileReader

import pathlib
import os
import json

from prefect_gcp.cloud_storage import GcsBucket

from prefect_gcp import GcpCredentials


@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_data(p: Pipeline):
    # print("Extracting Data...")
    return p.fetch_data()


@task(
    # cache_key_fn=task_input_hash,
    # cache_expiration=timedelta(days=1),
)
def fetch_chunks(filepath: str, chunk_size: int):
    return read_csv(filepath, iterator=True, chunksize=chunk_size)


@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def transform_data(p: Pipeline, chunk: DataFrame):
    return p.data_transform(chunk)


@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def insert_data(p: Pipeline, chunk: DataFrame):
    p.insert_rows(chunk)


@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def create_table(p: Pipeline, chunk: DataFrame):
    p.create_table(chunk)


# ---------- GCP Code -------------------------
@task(retries=3)
def fetch(url: str) -> DataFrame:
    return pd.read_csv(url)


@task(log_prints=True)
def clean(df: DataFrame) -> DataFrame:
    # Make columns lowercase
    df.columns = [str(column).lower() for column in df.columns]

    # Convert string datetime into pandas datetime
    if "tpep_pickup_datetime" in df.columns:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    if "tpep_dropoff_datetime" in df.columns:
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    if "lpep_pickup_datetime" in df.columns:
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    if "lpep_dropoff_datetime" in df.columns:
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    # Filter data of trips that has zero passengers
    # df = df[df["passenger_count"] != 0]

    return df


@task
def write_parquet_file(df: DataFrame, filename: str) -> pathlib.Path:
    path = pathlib.Path(f"data/{filename}.parquet")
    if not os.path.exists("data"):
        os.makedirs("data/")
    df.to_parquet(path=path, compression="gzip")
    return path


@task(retries=3)
def write_to_gcs(source: str, destination: str) -> None:
    gcs_block = GcsBucket.load("gcs-data-store")
    gcs_block.upload_from_path(from_path=source, to_path=destination)  # type: ignore typing does not work for this library


@task(retries=3)
def extract_from_gcs(path: str) -> pathlib.Path:
    # Get data from gcs and write to local filesystem
    dest = "./gcp"
    gcs_block = GcsBucket.load("gcs-data-store")
    gcs_block.get_directory(from_path=path, local_path=dest)  # type: ignore
    return pathlib.Path(f"{dest}/{path}")


@task
def create_schema_json(df: DataFrame, color: str, year: int, month: int):
    schema: dict = pd.io.json.build_table_schema(df)  # type: ignore
    if not os.path.exists("./schema"):
        os.makedirs("schema/")
    open(f"./schema/{color}_tripdata_{year}-{month:02}.json", "w+").write(
        json.dumps(schema)
    )


@task(log_prints=True)
def transform_gcs_data(path: pathlib.Path) -> DataFrame:
    df = pd.read_parquet(path)
    print(f"*************** {df.shape[0]} ")
    # print(
    #     f"before replacement: number of zero passenger trips: {df['passenger_count'].isna().sum()}"
    # )
    # df["passenger_count"].fillna(0, inplace=True)  # replace 0 values with NA
    # print(
    #     f"after after replacement: number of zero passenger trips: {df['passenger_count'].isna().sum()}"
    # )
    return df


@task
def upload_to_bq(df: DataFrame, color: str) -> None:
    gcp_credentials_block = GcpCredentials.load("service-account-cred")
    df.to_gbq(
        destination_table=f"de_zoomcamp_dataset.{color}_taxi_data",
        project_id="de-zoomcamp-376020",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),  # type: ignore
        chunksize=10_000,
        if_exists="append",
    )
