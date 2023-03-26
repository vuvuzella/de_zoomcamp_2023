from prefect import flow, task
from prefect_gcp import GcpCredentials, GcsBucket
from prefect.tasks import task_input_hash, exponential_backoff
from pathlib import Path
from datetime import timedelta
import zipfile
import os

BUCKET_NAME = "de-final-project-381710_fp-data-lake"


@task(
    log_prints=True,
    cache_result_in_memory=True,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(1),  # cache expires after 1 data
    retry_jitter_factor=1.5,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
)
def get_raw_data_from_gcs():
    gcp_credential_block = GcpCredentials.load("gcp-service-account")
    filename = "anz road crash.zip"
    from_path = f"anz_road_crash_data/source/{filename}"
    to_path = f"{Path(__file__).parent.parent.parent}/data/{filename}"
    bucket = GcsBucket(
        bucket=BUCKET_NAME,
        gcp_credentials=gcp_credential_block,  # type: ignore
    )
    return bucket.download_object_to_path(from_path=from_path, to_path=to_path)


@task(log_prints=True)
def unzip_file_to_destination(source: str, destination: str):
    with zipfile.ZipFile(source, "r") as zip_fp:
        zip_fp.extractall(destination)


@task(
    log_prints=True,
    cache_result_in_memory=True,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(1),  # cache expires after 1 data
    retry_jitter_factor=1.5,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
)
def upload_to_datalake(source: str, destination: str):
    gcp_credential_block = GcpCredentials.load("gcp-service-account")
    bucket = GcsBucket(bucket=BUCKET_NAME, gcp_credentials=gcp_credential_block)  # type: ignore
    files = [
        f
        for f in os.listdir(source)
        if f.split(".")[1].lower() == "csv"  # upload CSV files
    ]
    print(source)
    print(files)
    for file in files:
        from_path = f"{source}/{file}"
        to_path = f"{destination}/{file}"
        bucket.upload_from_path(from_path=from_path, to_path=to_path)  # type: ignore
        print(f"Successfully uploaded {file} to {to_path}")


@flow
def run_anz_road_crash_etl():
    # Download zip file from public repository
    # Extract the files into filesystem
    # Upload files to Datalake
    # Download the files from datalake
    # Create transform the csv files into datasets in Google BigQuery
    # Run the dbt command line to create the views and update the tables in BugQuery
    # End of Pipeline
    fp = get_raw_data_from_gcs()
    zip_destination = str(fp).split(".")[0]
    unzip_file_to_destination(str(fp), zip_destination)
    data_to_upload = (
        Path(zip_destination) / "anz_crash_20200903_fix" / "anz_crash_20200903_fix"
    )  # can't be bothered to fix file structure if source
    bucket_destination = "anz_road_crash_data/raw"
    upload_to_datalake(source=str(data_to_upload), destination=bucket_destination)


def main():
    run_anz_road_crash_etl()


if __name__ == "__main__":
    main()
