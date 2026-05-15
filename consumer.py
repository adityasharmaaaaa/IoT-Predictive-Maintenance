from kafka import KafkaConsumer
import pandas as pd
import json
from datetime import datetime

data_batch=[]
consumer=KafkaConsumer(
    'sensor_data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest'
)

for message in consumer:
    decoded=message.value.decode('utf-8')
    dict_data=json.loads(decoded)
    data_batch.append(dict_data)
    if len(data_batch)==10:
        df=pd.DataFrame(data_batch)
        filename = f"sensor_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.to_parquet(filename,index=False)
        print(f"Saved batch to {filename}")
        data_batch=[]
