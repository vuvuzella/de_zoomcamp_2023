import prefect
from prefect import flow, task
import sqlalchemy
from etl_project.flows import yellow_taxi_etl_flow


def main():
    print(f"Prefect version: {prefect.__version__}")
    print(f"sqlalchemy version: {sqlalchemy.__version__}")

    yellow_taxi_etl_flow()


if __name__ == "__main__":
    main()
