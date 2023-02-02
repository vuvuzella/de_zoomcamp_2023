import pandas as pd
from abc import ABC, abstractmethod
from argparse import ArgumentParser
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from time import time
from os import getenv
from pandas.io.parsers import TextFileReader
from pandas import DataFrame
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta


class DBConfig:
    """
    Gets config parameters from environment variables to be passed to the pipeline
    username
    password
    host
    db_name
    port
    """

    def __init__(self) -> None:
        self.parser = ArgumentParser()
        self.parser.add_argument(
            "--username",
            dest="username",
            type=str,
            help="Enter database username",
            required=False,
        )
        self.parser.add_argument(
            "--password",
            dest="password",
            type=str,
            help="Enter database password",
            required=False,
        )
        self.parser.add_argument(
            "--host",
            dest="host",
            type=str,
            help="Hostname of the database to use",
            required=False,
        )
        self.parser.add_argument(
            "--port",
            dest="port",
            type=str,
            help="Port of the database to use",
            required=False,
        )
        self.parser.add_argument(
            "--db_name",
            dest="db_name",
            type=str,
            help="Name of the database to use",
            required=False,
        )

        self.args = self.parser.parse_args()

    @property
    def username(self) -> str | None:
        return vars(self.args).get("username") or getenv("DB_USERNAME")

    @property
    def password(self) -> str | None:
        return vars(self.args).get("password") or getenv("DB_PASSWORD")

    @property
    def host(self) -> str | None:
        return vars(self.args).get("host") or getenv("DB_HOST")

    @property
    def port(self) -> str | None:
        return vars(self.args).get("port") or getenv("DB_PORT")

    @property
    def db_name(self) -> str | None:
        return vars(self.args).get("db_name") or getenv("DB")


def get_schema(dataframe: DataFrame, db_name: str, con: Engine | None = None):
    # get_schema is undocumented function https://github.com/pandas-dev/pandas/issues/9960
    return pd.io.sql.get_schema(dataframe, db_name, con)  # type: ignore


class Pipeline(ABC):
    """
    Class for ingesting data into a database
    """

    def __init__(self, source: str, dest_db_name: str, config: DBConfig) -> None:
        self.source = source
        self.dest_db = dest_db_name
        # https://docs.sqlalchemy.org/en/20/core/engines.html
        self.engine = create_engine(
            f"postgresql://{config.username}:{config.password}@{config.host}:{config.port}/{config.db_name}"
        )

    @abstractmethod
    def data_transform(self, dataframe: DataFrame) -> DataFrame:
        raise NotImplementedError

    def fetch_data_chunks(self, chunk_size: int = 1000):
        df_iter: TextFileReader = pd.read_csv(
            self.source, iterator=True, chunksize=chunk_size
        )
        return df_iter

    def create_table(self, dataframe: DataFrame):
        columns = dataframe.head(n=0)
        columns.columns = [str(column).lower() for column in columns.columns]
        # POSTGRES-specific DDL for the CSV
        print(get_schema(columns, self.dest_db, con=self.engine))
        return columns.to_sql(name=self.dest_db, con=self.engine, if_exists="replace")

    def insert_rows(self, chunk: DataFrame) -> int | None:
        chunk = self.data_transform(chunk)
        chunk.to_sql(name=self.dest_db, con=self.engine, if_exists="append")

    def ingest(self) -> None:
        # We want to separate the creation of the table and the insertion of the rows
        # To do this, we first get the first chunk and get the first row,
        # generate the DDL SQL to create the table and execute it to propagate
        # the changes to the database

        df_iter = self.fetch_data_chunks(1000)

        elapsed_time = 0
        for chunk in df_iter:

            start_time = time()
            if not self.engine.dialect.has_table(self.engine, self.dest_db):
                self.create_table(chunk)
                self.insert_rows(chunk.tail(chunk.size - 1))
            else:
                self.insert_rows(chunk)
            end_time = time()

            time_to_insert = end_time - start_time
            elapsed_time = elapsed_time + time_to_insert

            print(f"inserted chunk, took {(time_to_insert):.3f} seconds")

        print(f"Done ingesting data to database. Elapsed Time is {elapsed_time:.3f}")


class YellowTaxiDataPipeline(Pipeline):
    def data_transform(self, dataframe: pd.DataFrame):
        # Make columns lowercase
        dataframe.columns = [str(column).lower() for column in dataframe.columns]
        # Convert string datetime into pandas datetime
        dataframe.tpep_pickup_datetime = pd.to_datetime(dataframe.tpep_pickup_datetime)
        dataframe.tpep_dropoff_datetime = pd.to_datetime(
            dataframe.tpep_dropoff_datetime
        )
        # Filter data of trips that has zero passengers
        dataframe = dataframe[dataframe["passenger_count"] != 0]
        return dataframe


class GreenTaxiDataPipeline(Pipeline):
    def data_transform(self, dataframe: pd.DataFrame):
        # Make columns lowercase
        dataframe.columns = [str(column).lower() for column in dataframe.columns]
        # Convert string datetime into pandas datetime
        dataframe.lpep_pickup_datetime = pd.to_datetime(dataframe.lpep_pickup_datetime)
        dataframe.lpep_dropoff_datetime = pd.to_datetime(
            dataframe.lpep_dropoff_datetime
        )
        return dataframe


class TaxiZonePipeline(Pipeline):
    def data_transform(self, dataframe: pd.DataFrame):
        # No transformation
        return dataframe
