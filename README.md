# 🏭 IoT Predictive Maintenance

A real-time machine health monitoring system that uses a deep learning LSTM Autoencoder to detect anomalies in IoT sensor data streamed through Apache Kafka. Abnormal temperature readings that could indicate equipment failure are flagged instantly and visualized on a live Streamlit dashboard.

---

## How It Works

The system runs as four concurrent components:

1. **Producer** — Simulates an IoT sensor by publishing temperature readings to a Kafka topic (`sensor_data`) every second. With a 5% probability it injects an overheating event (100–120 °C); otherwise it sends a normal reading (60–80 °C).

2. **Consumer** — Subscribes to the Kafka topic and batches every 10 messages into a timestamped Parquet file for offline training.

3. **Live Inference** — Loads the trained LSTM Autoencoder and scaler, then listens to the Kafka stream in real time. It maintains a sliding window of the last 10 temperature readings, runs inference on each window, and writes the result (timestamp, temperature, reconstruction error, and `NORMAL`/`ANOMALY` status) to `system_log.csv`.

4. **Dashboard** — A Streamlit app that reads `system_log.csv` and refreshes every second to display the current temperature, the model's reconstruction error, an alert banner on anomaly detection, and a live temperature chart.

---

## Architecture

```
IoT Sensor (producer.py)
        │
        ▼  Kafka Topic: sensor_data
 ┌──────────────┐
 │  consumer.py │ ──► .parquet files ──► train.py ──► anomaly_model.keras
 └──────────────┘                                          scaler.gz
        │
 ┌──────────────────┐
 │ live_inference.py│ ──► system_log.csv
 └──────────────────┘
        │
 ┌──────────────┐
 │ dashboard.py │ ──► Streamlit UI
 └──────────────┘
```

---

## The Model

The anomaly detection model is an **LSTM Autoencoder** trained on normal temperature sequences.

- **Input:** Sliding window of 10 consecutive temperature readings, normalized with `MinMaxScaler`
- **Architecture:** `LSTM(16) → RepeatVector(10) → LSTM(16) → TimeDistributed(Dense(1))`
- **Training:** 50 epochs on normal operating data with mean-squared-error loss
- **Anomaly threshold:** 95th percentile of reconstruction errors on the training set (`0.0867`)
- **Detection logic:** If the mean squared reconstruction error for a window exceeds the threshold, an `ANOMALY` is declared

---

## Repository Structure

```
IoT-Predictive-Maintenance/
├── train.py              # Train the LSTM Autoencoder on Parquet data
├── producer.py           # Kafka producer simulating IoT sensor
├── consumer.py           # Kafka consumer that saves data to Parquet files
├── live_inference.py     # Real-time anomaly detection from Kafka stream
├── dashboard.py          # Streamlit monitoring dashboard
├── anomaly_model.keras   # Pre-trained LSTM Autoencoder model
├── scaler.gz             # Fitted MinMaxScaler (saved with joblib)
├── data.zip              # Training dataset (Parquet files)
└── .gitignore
```

---

## Prerequisites

- Python 3.9+
- Apache Kafka running locally on `localhost:9092`
- Java (required by Kafka)

### Install dependencies

```bash
pip install tensorflow keras scikit-learn pandas numpy kafka-python joblib streamlit pyarrow
```

---

## Quick Start

> Run each of the following in a **separate terminal**.

### 1. Start Kafka

Make sure Kafka and ZooKeeper are running. Using a local installation:

```bash
# Start ZooKeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka broker
bin/kafka-server-start.sh config/server.properties
```

### 2. (Optional) Retrain the model

If you want to train from scratch using the included dataset:

```bash
unzip data.zip
python train.py
```

This will produce `anomaly_model.keras` and `scaler.gz`.

### 3. Start the sensor producer

```bash
python producer.py
```

### 4. Start live inference

```bash
python live_inference.py
```

This will begin writing `system_log.csv` as anomaly predictions are made.

### 5. Launch the dashboard

```bash
streamlit run dashboard.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser to view the live monitoring UI.

---

## Dashboard

The Streamlit dashboard refreshes every second and displays:

| Metric | Description |
|---|---|
| **Current Temperature** | Latest reading from the sensor |
| **AI Error Rate** | LSTM reconstruction error for the current window |
| **System Status** | ✅ System Healthy or 🚨 OVERHEATING DETECTED |
| **Temperature Chart** | Live line chart of the last 50 readings |

---

## Tech Stack

| Component | Technology |
|---|---|
| Message streaming | Apache Kafka (`kafka-python`) |
| Deep learning model | TensorFlow / Keras (LSTM Autoencoder) |
| Data processing | Pandas, NumPy, scikit-learn |
| Data storage | Apache Parquet (`pyarrow`) |
| Dashboard | Streamlit |
| Model persistence | Keras `.keras` format + `joblib` |

---

## Notes

- The pre-trained `anomaly_model.keras` and `scaler.gz` are included so you can skip training and run inference immediately.
- The anomaly threshold of `0.0867` was derived from the 95th percentile of training reconstruction errors. Adjust it in `live_inference.py` to tune sensitivity.
- `live_inference.py` disables GPU usage by default to ensure compatibility with Apple Silicon Macs. Remove the `tf.config.set_visible_devices([], 'GPU')` block if you want GPU acceleration.
