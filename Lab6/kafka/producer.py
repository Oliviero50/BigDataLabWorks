
from kafka import KafkaProducer
import time
import random
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
recv_topic = 'cartopic'


def generateRandomData():
    carType = ["LKW", "PKW"]
    highway = ["A1", "A2", "A3"]
    return {
        "type": random.choice(carType),
        "highway": random.choice(highway),
        "timestamp": int(time.time())
    }


while True:
    data = json.dumps(generateRandomData())
    producer.send(recv_topic, bytes(data, 'utf-8'))
    print("send ... " + data + " ... to topic " + recv_topic)
    time.sleep(1)
