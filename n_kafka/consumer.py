from kafka import KafkaConsumer, consumer
from confluent_kafka import Consumer, KafkaError
from time import sleep
import json
import os


def save_to_json_file(file_addr, data):
    with open(file_addr, "r") as jf:
        cur_data = json.load(jf)
    if (isinstance(cur_data, list)):
        cur_data.append(data)
    elif (isinstance(cur_data, dict)):
        cur_data = data
    with open(file_addr, "w") as jf:
        json.dump(cur_data, jf)

class MessageConsumer:
    broker = ""
    topic = ""
    group_id = ""
    logger = None

    def __init__(self, broker, topic, group_id):
        self.broker = broker
        self.topic = topic
        self.group_id = group_id
        self.consumer = Consumer({
            'bootstrap.servers': self.broker,
            'group.id': self.group_id,  # Change to your consumer group ID
            'auto.offset.reset': 'earliest',  # Start consuming from the beginning of the topic
        })
        self.consumer.subscribe([self.topic])
    def activate_listener(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                    else:
                        print(f"Error while consuming message: {msg.error()}")
                else:
                    # Process the received message
                    print(f"Received message: {msg.value().decode('utf-8')}")

        except KeyboardInterrupt:
            print("Consumer interrupted by user.")
        finally:
            # Close the consumer upon exit
            self.consumer.close()
