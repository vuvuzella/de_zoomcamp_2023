from prefect import flow
from tasks import (
    extract_data,
    insert_data,
    fetch_chunks,
    transform_data,
    create_table,
    fetch,
    clean,
    write_parquet_file,
    write_to_gcs,
    extract_from_gcs,
    transform_gcs_data,
    upload_to_bq,
    create_schema_json,
)
from pipelines import YellowTaxiDataPipeline
from config import DBConfig
from argparse import Namespace
from time import time
import os
import pandas as pd

from prefect_gcp.cloud_storage import GcsBucket


@flow(name="Yellow Taxi Data ETL", log_prints=True)
def yellow_taxi_etl_flow():
    db_config = DBConfig(
        Namespace(
            username="de_zoomcamp",
            password="de_zoomcamp",
            db_name="de_zoomcamp",
            host="localhost",
            port="5433",
        )
    )
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    yellow_taxi_pipeline = YellowTaxiDataPipeline(
        source=url, dest_table_name="yellow_taxi_data", config=db_config
    )

    filepath = f"{os.path.dirname(__file__)}/yellow_tripdata_2021-01.csv.gz"
    if os.path.exists(filepath):
        # do nothing
        ...
    else:
        # fetch data
        filepath = extract_data(yellow_taxi_pipeline)

    df_iter = yellow_taxi_pipeline.fetch_chunks(filepath)

    elapsed_time = 0

    for chunk in df_iter:

        start_time = time()
        # transform data
        processed_data = transform_data(yellow_taxi_pipeline, chunk)

        if not yellow_taxi_pipeline.is_table_exist():
            # create table
            create_table(yellow_taxi_pipeline, processed_data)
            # insert data n-1
            insert_data(
                yellow_taxi_pipeline, processed_data.tail(n=processed_data.size - 1)
            )
        else:
            # transform all data
            insert_data(yellow_taxi_pipeline, processed_data)

        end_time = time()

        time_to_insert = end_time - start_time
        elapsed_time = elapsed_time + time_to_insert

        print(f"inserted chunk, took {(time_to_insert):.3f} seconds")

    print(f"Done ingesting data to database. Elapsed Time is {elapsed_time:.3f}")


@flow(name="ETL Web To GCP", log_prints=True)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    print(f"size: {df.shape[0]}")
    clean_df = clean(df)
    create_schema_json(clean_df, color, year, month)
    path = write_parquet_file(clean_df, dataset_file)
    write_to_gcs(str(path), str(path))


@flow(name="ETL GCP to BigQuery", log_prints=True)
def etl_gcp_to_BigQuery(year: int, month: int, color: str) -> None:
    # Get from bigquery
    # Make additional transformations
    # Upload to Google BigQuery
    filename = f"{color}_tripdata_{year}-{month:02}.parquet"
    gcp_path = f"data/{filename}"

    local_path = extract_from_gcs(gcp_path)
    df = transform_gcs_data(local_path)
    upload_to_bq(df, color)


@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(month=month, year=year, color=color)

    for month in months:
        etl_gcp_to_BigQuery(month=month, year=year, color=color)
