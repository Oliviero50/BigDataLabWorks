from kafka import KafkaConsumer
import json.decoder
import time


# topic_to_consume = 'cartopic112'
topic_to_consume = 'pyspark'

consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'], auto_offset_reset='latest', group_id=None)
print("consuming ... " + topic_to_consume)
consumer.subscribe([topic_to_consume])

print('waiting for messages...')

for msg in consumer:
    print(msg)
