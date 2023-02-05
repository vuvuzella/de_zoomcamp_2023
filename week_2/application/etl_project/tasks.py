from datetime import timedelta
from prefect import task
from prefect.tasks import task_input_hash
from pandas.io.parsers import TextFileReader
from pandas import DataFrame, read_csv
from pipelines import Pipeline


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
