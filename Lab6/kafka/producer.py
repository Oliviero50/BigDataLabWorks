from kafka import KafkaProducer
import time
import random

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

counter = ["Apple", "Cat", "Dog"]

while True:
   producer.send('testtopic', bytes(random.choice(counter) + " Hallo", 'utf-8'))
   print("send...")
   time.sleep(2)