import prefect
from prefect import flow, task
import sqlalchemy
from flows import yellow_taxi_etl_flow


def main():
    yellow_taxi_etl_flow()


if __name__ == "__main__":
    main()
