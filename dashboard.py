import streamlit as st
import pandas as pd
import time

# Set up the webpage
st.set_page_config(page_title="IoT Factory Monitor", page_icon="🏭", layout="wide")
st.title("🏭 Real-Time IoT Predictive Maintenance")
st.markdown("Live sensor feed and Deep Learning anomaly detection.")

# Create placeholders for our UI elements so they can update live
metric_col1, metric_col2, metric_col3 = st.columns(3)
temp_metric = metric_col1.empty()
error_metric = metric_col2.empty()
status_metric = metric_col3.empty()

chart_placeholder = st.empty()

# Create a loop that updates the dashboard every 1 second
while True:
    try:
        # Read the log file written by our Kafka consumer
        df = pd.read_csv("system_log.csv").tail(50) # Grab the last 50 readings
        
        if not df.empty:
            latest = df.iloc[-1]
            
            # Update Metrics
            temp_metric.metric("Current Temperature", f"{latest['temperature']:.1f} °C")
            error_metric.metric("AI Error Rate", f"{latest['error']:.4f}")
            
            if latest['status'] == "ANOMALY":
                status_metric.error("🚨 OVERHEATING DETECTED")
            else:
                status_metric.success("✅ System Healthy")
            
            # Update Chart
            chart_placeholder.line_chart(df.set_index('timestamp')['temperature'])
            
    except Exception as e:
        pass
        
    time.sleep(1) # Wait 1 second before refreshing