from datetime import timedelta
from prefect import task
from prefect.tasks import task_input_hash
from pipelines import Pipeline

import pandas as pd
from pandas import DataFrame, read_csv
from pandas.io.parsers import TextFileReader

import pathlib
import os

from prefect_gcp.cloud_storage import GcsBucket


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
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    # Filter data of trips that has zero passengers
    # df = df[df["passenger_count"] != 0]

    return df


@task
def write_parquet_file(df: DataFrame, filename: str) -> pathlib.Path:
    path = pathlib.Path(f"data/{filename}.parquet")
    if not os.path.exists(path):
        os.makedirs("data/")
    df.to_parquet(path=path, compression="gzip")
    return path


@task(retries=3)
def write_to_gcs(source: str, destination: str) -> None:
    gcs_block = GcsBucket.load("gcs-data-store")
    gcs_block.upload_from_path(from_path=source, to_path=destination)  # type: ignore typing does not work for this library
