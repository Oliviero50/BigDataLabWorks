from kafka import KafkaProducer
import time
import json
import os
 
producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
#Replace the ip with your own if needed.
 
demo_data_file = "demo_data.txt"
file = open(os.path.join(demo_data_file))
topic_name = "motor_data"
 
while True:
    line = file.readline().rstrip()
    if not line:
        break
    else:
        print("send ... " + line + " ... to topic " + topic_name)
        producer.send(topic_name, bytes(line, 'utf-8'))
    time.sleep(1)
 
file.close()
print("End of file")
