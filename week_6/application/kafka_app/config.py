from pydantic import BaseSettings


class KafkaSettings(BaseSettings):

    bootstrap_servers: list[str] = ["localhost:9092"]
    kafka_topic: str = "rides_json"


SETTINGS = KafkaSettings()
