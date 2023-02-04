from prefect import flow
from etl_project.tasks import extract_data, ingest_data
from etl_project.pipelines import YellowTaxiDataPipeline
from etl_project.config import DBConfig
from argparse import Namespace


@flow(name="Yellow Taxi Data ETL")
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

    filepath = extract_data(yellow_taxi_pipeline)

    ingest_data(yellow_taxi_pipeline, filepath)
