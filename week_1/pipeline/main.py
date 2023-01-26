from upload_data import DBConfig, Pipeline


def main():
    # Get parameters
    # ingest data
    config = DBConfig()

    pipeline = Pipeline("files/yellow_tripdata_2021-01.csv.gz", config)
    pipeline.ingest()


if __name__ == "__main__":
    main()
