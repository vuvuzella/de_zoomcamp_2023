import prefect
from prefect import flow, task
import sqlalchemy
from flows import (
    yellow_taxi_etl_flow,
    etl_web_to_gcs,
    etl_gcp_to_BigQuery,
    etl_parent_flow,
)


def main():
    # yellow_taxi_etl_flow()
    # etl_web_to_gcs()

    color = "yellow"
    year = 2021
    months = [1, 2, 3]

    etl_parent_flow(months, year, color)


if __name__ == "__main__":
    main()
