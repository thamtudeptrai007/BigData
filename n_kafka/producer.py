
import json

from confluent_kafka import Producer

config = {
    'bootstrap.servers': "Phuongly:9092"
}

class ProjectProducer:
    broker = ""
    topic = ""
    producer = None

    def __init__(self, broker, topic):
        self.broker = broker
        self.topic = topic
        self.producer = Producer(config)

    def send_msg(self, msg):
        data_json = json.dumps(msg)
        self.producer.produce(self.topic, value=data_json.encode('utf-8'))
        self.producer.flush()
        # print(msg)
        print(' [x] Success send message to broker')

    # def bootstrap(self):
    #     f = open('films.json', encoding="utf8")
    #     data = json.load(f)
    #     for i in data:
    #         self.send_msg(i)
    #     print(" [x] message sent successfully...")
