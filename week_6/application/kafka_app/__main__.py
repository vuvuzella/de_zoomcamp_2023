from pathlib import Path

from kafka_app.config import SETTINGS

from kafka_app.models import GreenRide

from kafka_app.consumer_json import JsonConsumer
from kafka_app.producer_json import JsonProducer

import json


def producer():
    data_path = Path(__file__).parent.parent / "data" / "green_tripdata_2019-01.csv.gz"

    config = {
        "bootstrap_servers": SETTINGS.bootstrap_servers,
        "key_serializer": lambda key: str(key).encode(),
        "value_serializer": lambda x: json.dumps(x, default=str).encode("utf-8"),
    }

    producer = JsonProducer[GreenRide](publish_key="pu_location_id", **config)
    header, records = producer.read_records(str(data_path), is_gzip=True)
    producer.publish_rides(topic=SETTINGS.kafka_topic, messages=records)


def consumer():
    config = {
        "bootstrap_servers": SETTINGS.bootstrap_servers,
        "key_deserializer": lambda key: key.decode("utf-8"),
        "value_deserializer": lambda x: json.loads(
            x.decode("utf-8"),
            object_hook=lambda d: GreenRide(**d),
        ),
        "auto_offset_reset": "earliest",
        "enable_auto_commit": True,
    }
    consumer = JsonConsumer(**config)
    consumer.consume(SETTINGS.kafka_topic)


def main():

    # producer()
    consumer()


if __name__ == "__main__":
    main()
