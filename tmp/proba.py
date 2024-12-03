import pandas as pd
import os
import zipfile
from io import BytesIO
import paho.mqtt.client as mqtt

# Function to divide dataframe into N parts and zip them
def split_and_zip_dataframe(df, num_parts, zip_filename):
    chunk_size = len(df) // num_parts
    zip_buffer = BytesIO()

    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for i in range(num_parts):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size if i < num_parts - 1 else len(df)
            chunk = df.iloc[start_idx:end_idx]
            chunk_filename = f"part_{i+1}.csv"
            chunk.to_csv(chunk_filename, index=False)
            zipf.write(chunk_filename, arcname=chunk_filename)
            os.remove(chunk_filename)

    zip_buffer.seek(0)
    return zip_buffer.read()

# MQTT callback to send the zip file
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    df = pd.DataFrame({'A': range(1000), 'B': range(1000, 2000)})  # Example dataframe
    zip_data = split_and_zip_dataframe(df, 5, 'data.zip')  # Split into 5 parts and zip

    # Send the zipped data over MQTT
    client.publish("data/topic", zip_data)

# Initialize MQTT client
client = mqtt.Client()
client.on_connect = on_connect

# Connect to MQTT broker (change with actual broker details)
client.connect("mqtt.eclipse.org", 1883, 60)

# Start the MQTT loop
client.loop_forever()
