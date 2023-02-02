import prefect
from prefect import flow, task
import sqlalchemy


def main():
    print(f"Prefect version: {prefect.__version__}")
    print(f"sqlalchemy version: {sqlalchemy.__version__}")


if __name__ == "__main__":
    main()
