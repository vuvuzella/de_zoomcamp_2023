# This python is created using jupyter nbconvert --to script [notebook]
# this is already edited
import pandas as pd
from abc import ABC, abstractmethod
from argparse import ArgumentParser
from sqlalchemy import create_engine
from time import time
from os import getenv


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
    def username(self) -> str:
        return vars(self.args).get("username") or getenv("DB_USERNAME")

    @property
    def password(self) -> str:
        return vars(self.args).get("password") or getenv("DB_PASSWORD")

    @property
    def host(self) -> str:
        return vars(self.args).get("host") or getenv("DB_HOST")

    @property
    def port(self) -> str:
        return vars(self.args).get("port") or getenv("DB_PORT")

    @property
    def db_name(self) -> str:
        return vars(self.args).get("db_name") or getenv("DB")


class Pipeline(ABC):
    """
    Class for ingesting data into a database
    """

    def __init__(self, source: str, dest_db_name: str, config: DBConfig) -> None:
        self.source = source
        self.dest_db = dest_db_name
        # https://docs.sqlalchemy.org/en/20/core/engines.html
        self.engine = create_engine(
            f"postgresql://{config.username}:{config.password}@{config.host}:{config.port}/de_zoomcamp"
        )

    @abstractmethod
    def data_transform(self, dataframe: pd.DataFrame):
        NotImplementedError

    def ingest(self) -> None:

        # chunkify the csv, because reading the whole data into memory is costly
        df = pd.read_csv(self.source, iterator=False, nrows=100)

        # POSTGRES-specific DDL for the CSV
        print(pd.io.sql.get_schema(df, self.dest_db, con=self.engine))

        # We want to separate the creation of the table and the insertion of the rows
        # To do this, we first get the first chunk and get the first row,
        # generate the DDL SQL to create the table and execute it to propagate
        # the changes to the database

        chunk_size = 10000
        df_iter = pd.read_csv(self.source, iterator=True, chunksize=chunk_size)
        first_chunk = next(df_iter)

        # Create the table uisng the first row data of the csv which is the column names for the table
        columns = first_chunk.head(n=0)
        columns.to_sql(name=self.dest_db, con=self.engine, if_exists="replace")

        first_chunk_data = first_chunk.tail(n=chunk_size - 1)
        first_chunk_data.to_sql(name=self.dest_db, con=self.engine, if_exists="append")

        elapsed_time = 0
        for chunk in df_iter:
            start_time = time()

            # convert timestamp fields (tpep_pickup_datetime and tpep_dropoff_datetime) into datetime fields from test
            # TODO: This is very specific to our dataset. generalize this
            # chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)
            # chunk.tpep_dropoff_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime)

            chunk = self.data_transform(chunk)

            # insert rows
            chunk.to_sql(name=self.dest_db, con=self.engine, if_exists="append")

            end_time = time()
            time_to_insert = end_time - start_time
            elapsed_time = elapsed_time + time_to_insert

            print(f"inserted chunk, took {(time_to_insert):.3f} seconds")

        print(f"Done ingesting data to database. Elapsed Time is {elapsed_time:.3f}")


class TaxiDataPipeline(Pipeline):
    def data_transform(self, dataframe: pd.DataFrame):
        # Convert string datetime into pandas datetime
        dataframe.tpep_pickup_datetime = pd.to_datetime(dataframe.tpep_pickup_datetime)
        dataframe.tpep_dropoff_datetime = pd.to_datetime(
            dataframe.tpep_dropoff_datetime
        )
        return dataframe


class TaxiZonePipeline(Pipeline):
    def data_transform(self, dataframe: pd.DataFrame):
        # No transformation
        return dataframe
