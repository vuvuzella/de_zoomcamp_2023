from kafka import KafkaConsumer


class JsonConsumer:
    class ContextManager:
        def __init__(self, consumer: KafkaConsumer, topic: str) -> None:
            self.consumer = consumer
            self.topic = topic

        def __enter__(self):
            self.consumer.subscribe(self.topic)
            return True

        def __exit__(self, type, value, traceback):
            self.consumer.close()
            return True

    def __init__(self, **props) -> None:
        self.consumer = KafkaConsumer(**props)

    def subscribe(self, topic: str):
        print(f"Subscribed to {topic}")
        return self.ContextManager(self.consumer, topic)

    def consume(self, topic: str):
        self.consumer.subscribe(topic)

        with self.subscribe(topic):
            print(f"Consuming from {topic}")
            while True:
                try:
                    message = self.consumer.poll(1)
                    if not message:
                        continue
                    for key, val in message.items():
                        for msg_val in val:
                            print(f"{msg_val.key}:{msg_val.value}")
                except KeyboardInterrupt as e:
                    print(f"error: {e}")
                    break
                except Exception as e:
                    print(f"error: {e}")
                    continue
