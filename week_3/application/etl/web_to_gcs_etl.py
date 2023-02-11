from prefect import task, flow
from prefect.tasks import task_input_hash, exponential_backoff
from prefect_gcp import GcsBucket, GcpCredentials
from prefect_dask import DaskTaskRunner, get_async_dask_client
from datetime import timedelta
import os
import requests


@task(
    cache_result_in_memory=True,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(1),
    retries=2,
    log_prints=True,
    retry_jitter_factor=1.5,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
)
def extract_data(url: str):
    """
    Get data from web and write to filesystem
    """
    response = requests.get(url)
    print(f"status: {response.status_code}")
    if not os.path.exists("./data"):
        os.makedirs("./data")
    dest_path = f"./data/{url.split('/')[-1]}"
    open(dest_path, "wb").write(response.content)
    return dest_path


@task(
    cache_result_in_memory=True,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(1),
    retries=2,
    log_prints=True,
)
def upload_to_gcs(source: str, bucket: str):
    gcp_credentials_block = GcpCredentials.load("service-account-cred")
    gcs_block = GcsBucket(bucket=bucket, gcp_credentials=gcp_credentials_block)  # type: ignore
    gcs_dest_path = f"fhv/{source.split('/')[-1]}"
    result = gcs_block.upload_from_path(from_path=source, to_path=gcs_dest_path)  # type: ignore
    print(f"result from upload: {result}")


@flow(name="Web To GCS ETL Flow Subflow", log_prints=True)
def web_to_gcs_flow(url: str, bucket: str):
    extract_data_path = extract_data(url)
    print(f"extract_data_path: {extract_data_path}")
    upload_to_gcs(extract_data_path, bucket)
