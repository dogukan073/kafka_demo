from kafka import KafkaProducer
from time import sleep
import json


class Producer:
    def __init__(self, sleep_time: float = 0, topic: str = "topic1") -> None:
        self.producer = KafkaProducer(bootstrap_servers="localhost:9092")
        self.size = 10
        self.sleep_time = sleep_time
        self.topic = topic

    def kafka_python_producer_sync(self, msg: str):
        for i in range(self.size):
            val = {"msg": msg}
            val = json.dumps(val).encode("utf-8")
            future = self.producer.send(topic=self.topic, value=val, partition=i % 2)
            result = future.get(timeout=60)
            print(result)
            sleep(self.sleep_time)

        self.producer.flush()

    def start(self):
        print("Producer Start")
        self.kafka_python_producer_sync(msg="kafka")
        print("Producer End")
