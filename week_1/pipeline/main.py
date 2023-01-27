from upload_data import DBConfig, TaxiDataPipeline, TaxiZonePipeline


def main():
    # Get parameters
    # ingest data
    config = DBConfig()

    yellow_taxi_pipeline = TaxiDataPipeline(
        "files/yellow_tripdata_2021-01.csv.gz", "yellow_trip_data", config
    )
    yellow_taxi_pipeline.ingest()

    zone_pipeline = TaxiZonePipeline(
        "files/taxi+_zone_lookup.csv", "taxi_zone_data", config
    )
    zone_pipeline.ingest()


if __name__ == "__main__":
    main()
