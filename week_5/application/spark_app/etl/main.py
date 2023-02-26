from prefect import task, flow
import requests
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    TimestampType,
)
from pathlib import Path


@task
def get_data(url, filename: str, color: str, year: str) -> str | None:
    dest = f"data/raw/{color}/{year}"
    file_path = f"{dest}/{filename}"

    if not os.path.exists(dest):
        print(f"Creating folder {dest}")
        os.makedirs(dest)

    if not Path(file_path).is_file():
        print(f"Downloading {filename}")
        result = requests.get(url)
        if result.status_code == 200:
            open(file_path, "wb").write(result.content)
        else:
            print(
                f"Error processing {filename}: code: {result.status_code} message: {result.content}"
            )
            return None

    return file_path


@task
def create_spark_session():
    return SparkSession.builder.appName("default").master("local[*]").getOrCreate()


@task
def write_parquet(
    spark: SparkSession, fp: str, schema: StructType, color: str, year: str, month: int
):
    dest_path = f"data/pq/{color}/{year}/{month:02}"
    spark_df = spark.read.option("header", "true").schema(schema).csv(fp)
    if not Path(dest_path).is_dir():
        spark_df.repartition(8).write.parquet(dest_path)


@flow
def taxi_data_pipeline(
    colors: list[str], years: list[str], month_map: dict, schema_map: dict
):

    # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

    spark = create_spark_session()

    for color in colors:
        for year in years:
            months = month_map.get(year, [])
            for month in months:
                filename = f"{color}_tripdata_{year}-{month:02}.csv.gz"
                url = f"{base_url}/{color}/{filename}"
                fp = get_data(url, filename, color, year)
                if fp:
                    schema = schema_map.get(color, None)
                    if schema:
                        write_parquet(spark, fp, schema, color, year, month)
                    else:
                        print(
                            f"Skipping writing parquet for {filename}, no schema found"
                        )
                else:
                    raise Exception(f"{url} not found")


def main():
    print("Running Main")
    colors = ["yellow", "green"]
    years = ["2020", "2021"]
    month_map = {
        "2020": list(range(1, 13)),
        "2021": list(range(1, 8)),
    }
    schema_map = {
        "yellow": StructType(
            [
                StructField("VendorID", IntegerType(), True),
                StructField("tpep_pickup_datetime", TimestampType(), True),
                StructField("tpep_dropoff_datetime", TimestampType(), True),
                StructField("passenger_count", IntegerType(), True),
                StructField("trip_distance", DoubleType(), True),
                StructField("RatecodeID", IntegerType(), True),
                StructField("store_and_fwd_flag", StringType(), True),
                StructField("PULocationID", IntegerType(), True),
                StructField("DOLocationID", IntegerType(), True),
                StructField("payment_type", IntegerType(), True),
                StructField("fare_amount", DoubleType(), True),
                StructField("extra", DoubleType(), True),
                StructField("mta_tax", DoubleType(), True),
                StructField("tip_amount", DoubleType(), True),
                StructField("tolls_amount", DoubleType(), True),
                StructField("improvement_surcharge", DoubleType(), True),
                StructField("total_amount", DoubleType(), True),
                StructField("congestion_surcharge", DoubleType(), True),
            ]
        ),
        "green": StructType(
            [
                StructField("VendorID", IntegerType(), True),
                StructField("lpep_pickup_datetime", TimestampType(), True),
                StructField("lpep_dropoff_datetime", TimestampType(), True),
                StructField("store_and_fwd_flag", StringType(), True),
                StructField("RatecodeID", IntegerType(), True),
                StructField("PULocationID", IntegerType(), True),
                StructField("DOLocationID", IntegerType(), True),
                StructField("passenger_count", IntegerType(), True),
                StructField("trip_distance", DoubleType(), True),
                StructField("fare_amount", DoubleType(), True),
                StructField("extra", DoubleType(), True),
                StructField("mta_tax", DoubleType(), True),
                StructField("tip_amount", DoubleType(), True),
                StructField("tolls_amount", DoubleType(), True),
                StructField("ehail_fee", DoubleType(), True),
                StructField("improvement_surcharge", DoubleType(), True),
                StructField("total_amount", DoubleType(), True),
                StructField("payment_type", IntegerType(), True),
                StructField("trip_type", IntegerType(), True),
                StructField("congestion_surcharge", DoubleType(), True),
            ]
        ),
    }

    taxi_data_pipeline(colors, years, month_map, schema_map)
