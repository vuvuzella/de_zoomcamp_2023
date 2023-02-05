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
def etl_web_to_gcs() -> None:
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    clean_df = clean(df)
    path = write_parquet_file(clean_df, dataset_file)
    write_to_gcs(str(path), str(path))
