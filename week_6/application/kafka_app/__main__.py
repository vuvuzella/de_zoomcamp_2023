from kafka_app.producer_json import JsonProducer
from kafka_app.models import GreenRide
from pathlib import Path
from kafka_app.config import SETTINGS

import json


def main():

    data_path = Path(__file__).parent.parent / "data" / "green_tripdata_2019-01.csv.gz"

    config = {
        "bootstrap_servers": SETTINGS.bootstrap_servers,
        "key_serializer": lambda key: str(key).encode(),
        "value_serializer": lambda x: json.dumps(x.__dict__, default=str).encode(
            "utf-8"
        ),
    }

    producer = JsonProducer[GreenRide](publish_key="pu_location_id", **config)
    header, records = producer.read_records(str(data_path), is_gzip=True)
    producer.publish_rides(topic=SETTINGS.kafka_topic, messages=records)


if __name__ == "__main__":
    main()
