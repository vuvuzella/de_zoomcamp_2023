from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from typing import Dict, List, Tuple, Optional, TypeVar, Generic, NewType, get_args
from pathlib import Path

# from kafka_app.models import Ride, GreenRide
from pydantic import BaseModel

import csv
import gzip

TRide = TypeVar("TRide")
Ride = NewType("Ride", List[TRide])


class JsonProducer(Generic[TRide], KafkaProducer):

    # producer: Optional[KafkaProducer]
    publish_key: str

    def __init__(self, publish_key: str, **props: dict) -> None:
        # self.producer = KafkaProducer(**props) if props else None
        self.publish_key = publish_key
        super().__init__(**props)

    def read_records(
        self, resource_path: str, is_gzip: bool = False
    ) -> Tuple[List[str], List[TRide]]:

        generic_class = self.__orig_class__.__args__[0]  # type: ignore
        path = Path(resource_path)
        records: List[TRide] = []

        cm = (
            gzip.open(path, mode="rt", newline="")
            if is_gzip
            else open(path, mode="rt", newline="")
        )

        with cm as file:
            reader = csv.reader(file, delimiter=",")
            header = next(reader)

            count = 0
            for row in reader:
                zipped = {pair[0]: pair[1] for pair in zip(header, row)}
                ride = generic_class(**zipped)  # type: ignore
                records.append(ride)
                count += 1
                if count == 5:
                    return header, records
        return header, records

    # @staticmethod
    def publish_rides(self, topic: str, messages: List[TRide]):

        for msg in messages:
            try:
                record = self.send(topic=topic, key=self.publish_key, value=msg.json())  # type: ignore
                print(
                    f"Record {record} successfully produced at offset {record.get().offset}"
                )
            except KafkaTimeoutError as e:
                print(f"Kafka Error on publish: {e}")
                raise e
