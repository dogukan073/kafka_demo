from .base_consumer import Consumer


consumer = Consumer(partition=1)
consumer.start()
