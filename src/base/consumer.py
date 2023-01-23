from kafka import KafkaConsumer
from time import sleep


class Consumer:
    def __init__(
        self, topic: str = "topic1", sleep_time: float = 0, group_id: str = "codehub"
    ) -> None:
        self.consumer = KafkaConsumer(
            topic, bootstrap_servers="localhost:9092", group_id=group_id
        )
        print("Consumer Listen")
        self.sleep_time = sleep_time

    def kafka_python_consumer(self):
        # listens until stopped
        for msg in self.consumer:
            print(msg)
            self.consumer.commit()
            sleep(self.sleep_time)

    def start(self):
        print("Consumer Start")
        self.kafka_python_consumer()
