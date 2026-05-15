import random
import time
import json
from datetime import datetime
from kafka import KafkaProducer

producer=KafkaProducer(bootstrap_servers=['localhost:9092'])

while True:
    timestamp=datetime.now().isoformat()
    x=random.random()
    if(x<=0.05):
        temp=random.uniform(100.0,120.0)
    else:    
        temp=random.uniform(60.0,80.0)
    data={
        "machine_id":"Machine_1",
        "temperature":temp,
        "timestamp":timestamp
    }
    json_data=json.dumps(data)
    producer.send('sensor_data',json_data.encode('utf-8'))
    print(json_data)
    time.sleep(1)
