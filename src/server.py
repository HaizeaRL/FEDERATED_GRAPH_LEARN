import sys
import os
from pathlib import Path
import yaml
import time

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the module
from modules import mqtt_functions as mqttf

# READ DATA FROM CONFIG.YAML
root_path = "/usr/local/app/"
conf = yaml.safe_load(Path(os.path.join(root_path,"config.yaml")).read_text())

# CREATE CLIENT AND CONNECT TO PUBLIC BROKER
mqttc = mqttf.create_mqtt_client(conf)

# Record the start time
start_time = time.time()

# SUSCRIBE AND START LISTENING MESSAGES FOR A TIME
print("Server: waiting for messages...")
while time.time() - start_time < conf["TIME_LISTENING_MESSAGES"]:
    mqttc.loop_start()
    mqttc.subscribe(conf["MQTT_TOPIC"])
    mqttc.on_message = mqttf.on_message
    mqttc.loop_stop()

# After the loop ends, you can manage receiving messages here
print("Time's up! Starting to manage receiving messages...")
