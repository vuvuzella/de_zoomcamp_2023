from prefect import flow
from etl.web_to_gcs_etl import web_to_gcs_flow
from prefect_dask import DaskTaskRunner, get_async_dask_client
import asyncio


@flow(name="FHV NY Trip Data ETL", log_prints=True)
def fhv_flow(year_list: list[int], month_list: list[int]):
    bucket = f"de-zoomcamp-376020_week_3_data_store"
    for year in year_list:
        for month in month_list:
            url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz"
            print(f"************ url: {url}")
            web_to_gcs_flow(url, bucket)  # type: ignore


def main():
    fhv_flow([2019], [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])


if __name__ == "__main__":
    main()
