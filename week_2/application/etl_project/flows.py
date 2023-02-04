from prefect import flow
from etl_project.tasks import extract_data
from etl_project.pipelines import YellowTaxiDataPipeline
from etl_project.config import DBConfig


@flow(name="Yellow Taxi Data ETL")
def yellow_taxi_etl_flow():
    # url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    # db_config = DBConfig()
    # yellow_taxi_pipeline = YellowTaxiDataPipeline(
    #     source=url, dest_table_name="yellow_taxi_data", config=db_config
    # )
    extract_data()
