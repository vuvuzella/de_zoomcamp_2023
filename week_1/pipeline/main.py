from upload_data import (
    DBConfig,
    YellowTaxiDataPipeline,
    GreenTaxiDataPipeline,
    TaxiZonePipeline,
)


def main():
    # Get parameters
    # ingest data
    config = DBConfig()

    yellow_taxi_pipeline = YellowTaxiDataPipeline(
        "files/yellow_tripdata_2021-01.csv.gz", "yellow_trip_data", config
    )
    yellow_taxi_pipeline.ingest()

    green_taxi_pipeline = GreenTaxiDataPipeline(
        "files/green_tripdata_2019-01.csv.gz", "green_trip_data", config
    )
    green_taxi_pipeline.ingest()

    zone_pipeline = TaxiZonePipeline(
        "files/taxi+_zone_lookup.csv", "taxi_zone_data", config
    )
    zone_pipeline.ingest()


if __name__ == "__main__":
    main()
