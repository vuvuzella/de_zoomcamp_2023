import zipfile
import os
from pathlib import Path
from datetime import timedelta
from typing import List, Tuple

from prefect import flow, task
from prefect_gcp import GcpCredentials, GcsBucket
from prefect_gcp.bigquery import bigquery_insert_stream
from prefect.tasks import task_input_hash, exponential_backoff

from pyspark.sql import SparkSession
import pandas as pd
from pandas import StringDtype, BooleanDtype, DatetimeTZDtype, Float64Dtype, Int64Dtype
import pandas_gbq as pd_gbq
import numpy as np
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
import json
from enum import Enum
from google.cloud import bigquery

PROJECT_ID = "de-final-project-381710"
BUCKET_NAME = f"{PROJECT_ID}_fp-data-lake"
BQ_DATASET = "anz_road_crash_dataset"


class EXTENSION(Enum):
    CSV = "csv"
    PARQUET = "parquet"


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
def upload_to_datalake(
    source: Path, destination: Path, extension: EXTENSION = EXTENSION.CSV
):
    gcp_credential_block = GcpCredentials.load("gcp-service-account")
    bucket = GcsBucket(bucket=BUCKET_NAME, gcp_credentials=gcp_credential_block)  # type: ignore
    files = [
        f
        for f in os.listdir(source)
        if f.split(".")[1].lower() == extension.value  # upload CSV files
    ]
    for file in files:
        from_path = source / file
        to_path = destination / file
        bucket.upload_from_path(from_path=from_path, to_path=to_path)  # type: ignore
        print(f"Successfully uploaded {file} to {to_path}")
    return files


@task(
    log_prints=True,
    cache_result_in_memory=False,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(1),  # cache expires after 1 data
)
def csv_transform(
    base_dir: Path,
    files: List[Tuple[str, str, List[bigquery.SchemaField], List[int]]],
    out_dir: Path,
):
    out_dir.mkdir(parents=True, exist_ok=True)  # make sure output directory exists

    for file, table, schema, drop_cols in files:
        in_fp = base_dir / file
        out_fp = out_dir / file
        df_csv = pd.read_csv(str(in_fp), header=0)

        # basic transformations
        if len(drop_cols):
            df_csv = df_csv.drop(
                columns=[df_csv.columns[col] for col in drop_cols], axis=1
            )  # drop unwanted unnamed first column

        # write csv to out
        df_csv.to_csv(
            path_or_buf=out_fp, index=False
        )  # write transformed csv, compressed
        print(f"Successfully transformed csv: {file}")


@task(
    log_prints=True,
    # cache_result_in_memory=False,
    # cache_key_fn=task_input_hash,
    # cache_expiration=timedelta(1),  # cache expires after 1 data
)
def convert_to_parqet(base_dir: Path, files: List[str], out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)  # make sure output directory exists

    for file in files:
        in_fp = base_dir / file
        out_fp = out_dir / file.replace(".csv", ".parquet")
        df_csv = pd.read_csv(str(in_fp))
        df_csv.astype(str).to_parquet(path=str(out_fp), compression="gzip", index=False)
        print(f"Successfully converted to parquet: {file}")


@task(
    log_prints=False,
    cache_result_in_memory=False,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(1),  # cache expires after 1 data
    retry_jitter_factor=1.5,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
)
def upload_to_data_warehouse(
    base_dir: Path,
    file_table_tuples: List[Tuple[str, str, List[bigquery.SchemaField], List[int]]],
    project_id: str,
    dataset_id: str,
):
    gcp_credential_block = GcpCredentials.load("gcp-service-account")
    client = bigquery.Client(credentials=gcp_credential_block.get_credentials_from_service_account())  # type: ignore
    for filename, table, schema, drop_cols in file_table_tuples:
        fn = filename.replace(".csv", ".parquet")
        fp = f"gs://{base_dir / fn}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = client.load_table_from_uri(
            source_uris=str(fp),
            destination=f"{project_id}.{dataset_id}.{table}",
            job_config=job_config,
        )

        load_job.result()
        print(f"Successfully uploaded to BigQuery: {fn}")


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
    bucket_destination = Path("anz_road_crash_data") / "raw"

    upload_to_datalake(
        source=data_base_dir, destination=bucket_destination, extension=EXTENSION.CSV
    )

    file_table_tuples: List[Tuple[str, str, List[bigquery.SchemaField], List[int]]] = [
        (
            "Casualties.csv",
            "raw_casualties",
            [
                bigquery.SchemaField("casualties_id", "STRING"),
                bigquery.SchemaField("casualties", "STRING"),
                bigquery.SchemaField("fatalities", "STRING"),
                bigquery.SchemaField("serious_injuries", "STRING"),
                bigquery.SchemaField("minor_injuries", "STRING"),
            ],
            [0],
        ),
        (
            "Crash2.csv",
            "raw_crashes",
            [
                bigquery.SchemaField("crash_id", "STRING"),
                bigquery.SchemaField("lat_long", "STRING"),
                bigquery.SchemaField("date_time_id", "STRING"),
                bigquery.SchemaField("description_id", "STRING"),
                bigquery.SchemaField("vehicles_id", "STRING"),
                bigquery.SchemaField("casualties_id", "STRING"),
            ],
            [],  # columns to drop
        ),
        (
            "DateTime.csv",
            "raw_datetimes",
            [
                bigquery.SchemaField("date_time_id", "STRING"),
                bigquery.SchemaField("year", "STRING"),
                bigquery.SchemaField("month", "STRING"),
                bigquery.SchemaField("day_of_week", "STRING"),
                bigquery.SchemaField("day_of_month", "STRING"),
                bigquery.SchemaField("hour", "STRING"),
                bigquery.SchemaField("approximate", "STRING"),
            ],
            [0],  # skip unknown column
        ),
        (
            "Description.csv",
            "raw_descriptions",
            [
                bigquery.SchemaField("description_id", "STRING"),
                bigquery.SchemaField("severity", "STRING"),
                bigquery.SchemaField("speed_limit", "STRING"),
                bigquery.SchemaField("midblock", "STRING"),
                bigquery.SchemaField("intersection", "STRING"),
                bigquery.SchemaField("road_position_horizontal", "STRING"),
                bigquery.SchemaField("road_position_vertical", "STRING"),
                bigquery.SchemaField("road_sealed", "STRING"),
                bigquery.SchemaField("road_wet", "STRING"),
                bigquery.SchemaField("weather", "STRING"),
                bigquery.SchemaField("crash_type", "STRING"),
                bigquery.SchemaField("lighting", "STRING"),
                bigquery.SchemaField("traffic_controls", "STRING"),
                bigquery.SchemaField("drugs_alcohol", "STRING"),
                bigquery.SchemaField("DCA_code", "STRING"),
                bigquery.SchemaField("comment", "STRING"),
            ],
            [0],
        ),
        (
            "Location2.csv",
            "raw_locations",
            [
                bigquery.SchemaField("lat_long", "STRING"),
                bigquery.SchemaField("latitude", "STRING"),
                bigquery.SchemaField("longitude", "STRING"),
                bigquery.SchemaField("country", "STRING"),
                bigquery.SchemaField("state", "STRING"),
                bigquery.SchemaField("local_government_area", "STRING"),
                bigquery.SchemaField("statistical_area", "STRING"),
                bigquery.SchemaField("suburb", "STRING"),
            ],
            [],
        ),
        (
            "Vehicles.csv",
            "raw_vehicles",
            [
                bigquery.SchemaField("vehicles_id", "STRING"),
                bigquery.SchemaField("animals", "STRING"),
                bigquery.SchemaField("car_sedan", "STRING"),
                bigquery.SchemaField("car_utility", "STRING"),
                bigquery.SchemaField("car_van", "STRING"),
                bigquery.SchemaField("car_4x4", "STRING"),
                bigquery.SchemaField("car_station_wagon", "STRING"),
                bigquery.SchemaField("motor_cycle", "STRING"),
                bigquery.SchemaField("truck_small", "STRING"),
                bigquery.SchemaField("truck_large", "STRING"),
                bigquery.SchemaField("bus", "STRING"),
                bigquery.SchemaField("taxi", "STRING"),
                bigquery.SchemaField("bicycle", "STRING"),
                bigquery.SchemaField("scooter", "STRING"),
                bigquery.SchemaField("pedestrian", "STRING"),
                bigquery.SchemaField("inanimate", "STRING"),
                bigquery.SchemaField("train", "STRING"),
                bigquery.SchemaField("tram", "STRING"),
                bigquery.SchemaField("vehicle_other", "STRING"),
            ],
            [0],
        ),
    ]

    file_list = [pair[0] for pair in file_table_tuples]

    transform_output_path = Path(zip_destination) / "csv_transform"
    csv_transform(
        base_dir=data_base_dir, files=file_table_tuples, out_dir=transform_output_path
    )

    parquet_output_path = Path(zip_destination) / "parquet"
    convert_to_parqet(
        base_dir=transform_output_path,
        files=file_list,
        out_dir=parquet_output_path,
    )

    upload_to_datalake(
        source=parquet_output_path,
        destination=Path("anz_road_crash_data/parquet"),
        extension=EXTENSION.PARQUET,
    )

    uri_base_dir = Path(f"{BUCKET_NAME}/anz_road_crash_data/parquet")
    upload_to_data_warehouse(
        base_dir=uri_base_dir,
        file_table_tuples=file_table_tuples,
        project_id=PROJECT_ID,
        dataset_id=BQ_DATASET,
    )


def main():
    run_anz_road_crash_etl()


if __name__ == "__main__":
    main()

# Experimental Codes
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
