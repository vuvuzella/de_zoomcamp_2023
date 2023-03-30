import zipfile
import os
from pathlib import Path
from datetime import timedelta
from typing import List, Tuple

from prefect import flow, task
from prefect_gcp import GcpCredentials, GcsBucket
from prefect.tasks import task_input_hash, exponential_backoff

from pyspark.sql import SparkSession
import pandas as pd
import pandas_gbq as pd_gbq

BUCKET_NAME = "de-final-project-381710_fp-data-lake"


@task(
    log_prints=True,
    cache_result_in_memory=True,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(1),  # cache expires after 1 day
    retry_jitter_factor=1.5,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
)
def get_raw_data_from_gcs():
    # TODO: Generalize this so that it can be used by other parts of the flow
    gcp_credential_block = GcpCredentials.load("gcp-service-account")
    filename = "anz_road_crash.zip"
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
    return files


# @task
# def csv_to_parquet(source: str, destination: str):
#     files = [
#         f
#         for f in os.listdir(source)
#         if f.split(".")[1].lower() == "csv"  # upload CSV files
#     ]
#     spark = (
#         SparkSession.builder.appName("prefect-csv-to-parquet")
#         # .master("local[*]")
#         .master("spark://localhost:7077").getOrCreate()
#     )
#     print("******** start iter **********")
#     for file in files:
#         print(f"**** file: {file}")
#         fp = (
#             Path(os.path.abspath("")).parent
#             # Path("/opt/workspace")
#             / "data/anz_road_crash/anz_crash_20200903_fix/anz_crash_20200903_fix"
#             / file
#         )
#         df = spark.read.option("header", "True").csv(f"file:///{str(fp)}")
#         print(df)

# schema_casualty = StructType(
#     List(
#         # StructField(_c0,StringType,true), # Remove this column
#         StructField(name=casualties_id, dataType=StringType, nullable=true),
#         StructField(name=casualties, dataType=StringType, nullable=true),
#         StructField(name=fatalities, dataType=StringType, nullable=true),
#         StructField(name=serious_injuries, dataType=StringType, nullable=true),
#         StructField(name=minor_injuries, dataType=StringType, nullable=true),
#     )
# )

# schema_crash = StructType(
#     List(
#         StructField(name=crash_id, dataType=StringType, nullable=true),
#         StructField(name=lat_long, dataType=StringType, nullable=true),
#         StructField(name=date_time_id, dataType=StringType, nullable=true),
#         StructField(name=description_id, dataType=StringType, nullable=true),
#         StructField(name=vehicles_id, dataType=StringType, nullable=true),
#         StructField(name=casualties_id, dataType=StringType, nullable=true),
#     )
# )
@task
@task
def upload_to_data_warehouse(base_dir: str, file_table_tuples: List[Tuple[str, str]]):
    gcp_credential_block = GcpCredentials.load("gcp-service-account")
    dataset_id = "anz_road_crash_dataset"
    for filename, table in file_table_tuples:
        fp = Path(base_dir) / filename
        df = pd.read_csv(filepath_or_buffer=str(fp), header=0)
        cols = df.columns
        df = df.drop(columns=cols[0])
        # TODO: convert to parquet first, then upload to google bigquery
        df.astype(str).to_gbq(
            destination_table=f"{dataset_id}.{table}",
            project_id="de-final-project-381710",
            chunksize=1000,
            credentials=gcp_credential_block.get_credentials_from_service_account(),  # type: ignore
            if_exists="replace",
        )


@task
def run_dbt_tasks():
    # TODO: Use dbt core cli library to run dbt tasks and perform transformations to the warehouse
    ...


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
    data_base_dir = (
        Path(zip_destination) / "anz_crash_20200903_fix" / "anz_crash_20200903_fix"
    )  # can't be bothered to fix file structure if source
    bucket_destination = "anz_road_crash_data/raw"
    upload_to_datalake(source=str(data_base_dir), destination=bucket_destination)
    file_table_tuples = [
        ("Casualties.csv", "raw_casualties"),
        ("Crash2.csv", "raw_crashes"),
        ("DateTime.csv", "raw_datetimes"),
        ("Description.csv", "raw_descriptions"),
        ("Location2.csv", "raw_locations"),
        ("Vehicles.csv", "raw_vehicles"),
    ]
    upload_to_data_warehouse(
        base_dir=str(data_base_dir), file_table_tuples=file_table_tuples
    )


def main():
    run_anz_road_crash_etl()


if __name__ == "__main__":
    main()
