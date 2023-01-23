from kafka import KafkaConsumer, TopicPartition


class Consumer:
    def __init__(self, topic: str = "topic2", partition: int = 0) -> None:
        self.consumer = KafkaConsumer(
            bootstrap_servers="localhost:9092", group_id="group"
        )
        self.consumer.assign([TopicPartition(topic, partition)])
        print("Consumer Listen")

    def kafka_python_consumer(self):
        # listens until stopped
        for msg in self.consumer:
            print(msg)
            self.consumer.commit()

    def start(self):
        print("Consumer Start")
        self.kafka_python_consumer()
