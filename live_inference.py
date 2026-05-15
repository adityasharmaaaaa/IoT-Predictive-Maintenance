import os
import json
import numpy as np
from collections import deque
from kafka import KafkaConsumer
import joblib
import datetime

import tensorflow as tf
try:
    tf.config.set_visible_devices([], 'GPU')
    print("Mac GPU successfully disabled for inference.")
except Exception as e:
    pass
# ------------------------

from tensorflow.keras.models import load_model

print("Loading AI Model and Scaler...")
# compile=False is crucial. It stops TF from loading training optimizers that freeze Macs.
model = load_model("anomaly_model.keras", compile=False)
scaler = joblib.load("scaler.gz")

THRESHOLD = 0.0867  

consumer = KafkaConsumer(
    'sensor_data',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')) 
)

window = deque(maxlen=10)

print("Listening for live machine data...")

with open("system_log.csv", "w") as f:
    f.write("timestamp,temperature,error,status\n")

for message in consumer:
    data = message.value
    current_temp = data['temperature']
    
    window.append(current_temp)
    
    if len(window) == 10:
        temp_array = np.array(window).reshape(-1, 1)
        scaled_window = scaler.transform(temp_array)
        
        model_input = scaled_window.reshape(1, 10, 1)
        
        prediction = model.predict(model_input, verbose=0)
        
        error = np.mean(np.square(model_input - prediction))
        
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        
        if error > THRESHOLD:
            print(f"🚨 ALARM! Temp: {current_temp:.1f}°")
            status = "ANOMALY"
        else:
            print(f"✅ Normal. Temp: {current_temp:.1f}°")
            status = "NORMAL"
            
        with open("system_log.csv", "a") as f:
            f.write(f"{timestamp},{current_temp},{error:.4f},{status}\n")