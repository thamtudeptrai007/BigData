import os
from dotenv import load_dotenv
from n_kafka.consumer import MessageConsumer
load_dotenv()
broker = "Phuongly:9092"
topic = "project"
group_id = "consumer-1"
consumer1 = MessageConsumer(broker=broker, topic=topic, group_id=group_id)
consumer1.activate_listener()
