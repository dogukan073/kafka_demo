from src.base.consumer import Consumer


consumer = Consumer(sleep_time=0.4, group_id="patates")
consumer.start()
