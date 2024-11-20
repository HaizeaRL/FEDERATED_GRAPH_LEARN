import time
import os
import random 
import json
import yaml
from pathlib import Path
import sys

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the module
from modules import mqtt_functions as mqttf

# create corresponding paths
root_path = "/usr/local/app/"
tmp_path = os.path.join(root_path, "tmp")

# read role file
role_file = open(os.path.join(tmp_path,"role.txt"), "r") 
role = role_file.read().strip()
role_file.close() 

# obtain id and execute code
id = None
if "_" in role:
    id = role.split("_")[1]
    if id:  
        
        # READ DATA FROM CONFIG.YAML
        conf = yaml.safe_load(Path(os.path.join(root_path,"config.yaml")).read_text())

        # CREATE CLIENT AND CONNECT TO PUBLIC BROKER
        mqttc = mqttf.create_mqtt_client(conf)
              
        while True:

            # PREPARE MESSAGE
            # create and send periodically a message
            n = random.randint(1, 200)

            # compose and create sending json
            update = {"Client": f"Client_{id}", "value": n}
            message = json.dumps(update)
            
            # SEND MESSAGE
            mqttc.publish(conf["MQTT_TOPIC"], message)
            print(f"Published {message} in {conf['MQTT_TOPIC']} topic.")
                
            time.sleep(1)

