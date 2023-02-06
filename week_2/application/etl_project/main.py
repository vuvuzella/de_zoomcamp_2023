import prefect
from prefect import flow, task
import sqlalchemy
from flows import yellow_taxi_etl_flow, etl_web_to_gcs, etl_gcp_to_BigQuery


def main():
    # yellow_taxi_etl_flow()
    # etl_web_to_gcs()
    etl_gcp_to_BigQuery()


if __name__ == "__main__":
    main()
