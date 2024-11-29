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
from modules import data_preparation_functions as dpf

# create corresponding paths
root_path = "/usr/local/app/"
tmp_path = os.path.join(root_path, "tmp")

# read role file
role_file = open(os.path.join(tmp_path,"role.txt"), "r") 
role = role_file.read().strip()
role_file.close() 

# read data_config file
data_conf = yaml.safe_load(Path(os.path.join(root_path,"data_config.yaml")).read_text())

# obtain id and execute code
id = None
if "_" in role:
    id = role.split("_")[1]
    if id:

        # select corresponding theme 
        theme = data_conf["themes"][id]  
        print(f"Client{id}: Selected theme is: {theme} downloading...")
        
        # unzip corresponding data
        data_path = os.path.join(root_path,"data")
        tar_file_path = os.path.join(data_path, "PHEME_veracity.tar")
        dpf.untar_specific_theme_data(tar_file_path, theme)
        print(f"Client{id}: Folder: {theme} and its subfolders exracted to data_path.")
       
        # prepare data for analysis
        print(f"Client{id}: Preparing data for analysis. Step 1: cleaning...")

        # 1- clean not desirable files if exists. Recursively        
        root_path = os.path.join(data_path,"all-rnr-annotated-threads") # Entry point
        dpf.clean_directory_recursively(root_path)

        # 2- Feature selection. Recursively  
        print(f"Client{id}: Step 2: selecting and extracting features from messages ...")
        # Example Execution
        theme_path = os.path.join(root_path,theme)

        # create folder to save preprocess data
        out_folder = os.path.join(theme_path, "preprocess")
        os.makedirs(out_folder, exist_ok=True)

        # Start processing
        dpf.preprocess_data(theme_path, out_folder, False)
        input()
        


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

