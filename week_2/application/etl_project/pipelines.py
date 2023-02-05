import pandas as pd
import requests
import sys
from validators.url import url
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from time import time
from pandas.io.parsers import TextFileReader
from pandas import DataFrame
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta
from abc import ABC, abstractmethod
from os.path import dirname
from prefect import flow

from config import DBConfig


def get_schema(dataframe: DataFrame, db_name: str, con: Engine | None = None):
    # get_schema is undocumented function https://github.com/pandas-dev/pandas/issues/9960
    return pd.io.sql.get_schema(dataframe, db_name, con=con)  # type: ignore


class Pipeline(ABC):
    """
    Class for ingesting data into a database
    """

    def __init__(self, source: str, dest_table_name: str, config: DBConfig) -> None:
        self.source = source
        self.dest_table = dest_table_name
        # TODO: Dependency to the sqlalchemy Engine makes this difficult to unit test.
        # https://docs.sqlalchemy.org/en/20/core/engines.html
        self.engine = create_engine(
            f"postgresql://{config.username}:{config.password}@{config.host}:{config.port}/{config.db_name}"
        )

    def is_table_exist(self) -> bool:
        return self.engine.has_table(self.dest_table)

    def data_transform(self, dataframe: DataFrame) -> DataFrame:
        return dataframe

    def fetch_data(self) -> str:
        # retrieve data, write data to file system, and return the file path to that file
        output_filename = f"{dirname(__file__)}/{self.source.split('/')[-1]}"
        response = requests.get(self.source)
        contents = response.content
        open(output_filename, "wb").write(contents)
        return output_filename

    def fetch_chunks(self, file_path: str, chunk_size: int = 1000) -> TextFileReader:
        df_iter = pd.read_csv(file_path, iterator=True, chunksize=chunk_size)
        return df_iter

    def create_table(self, dataframe: DataFrame) -> int | None:
        columns = dataframe.head(n=0)
        columns.columns = [str(column).lower() for column in columns.columns]
        # POSTGRES-specific DDL for the CSV
        print(get_schema(columns, self.dest_table, con=self.engine))
        return columns.to_sql(
            name=self.dest_table, con=self.engine, if_exists="replace"
        )

    def insert_rows(self, chunk: DataFrame) -> int | None:
        # chunk = self.data_transform(chunk)
        chunk.to_sql(name=self.dest_table, con=self.engine, if_exists="append")

    def ingest(self, df_iter: TextFileReader) -> None:

        elapsed_time = 0
        for chunk in df_iter:

            start_time = time()
            if not self.engine.has_table(self.dest_table):
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
    ...
