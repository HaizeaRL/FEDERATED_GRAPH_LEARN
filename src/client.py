import os
import yaml
from pathlib import Path
import sys
import pickle

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the module
from modules import mqtt_functions as mqttf
from modules import data_preparation_functions as dpf
from modules import graph_creation_analysis_functions as gcaf

# create corresponding paths
root_path = "/usr/local/app/"
tmp_path = os.path.join(root_path, "tmp")

# read role file
role_file = open(os.path.join(tmp_path,"role.txt"), "r") 
role = role_file.read().strip()
role_file.close() 

# read data_config file
data_conf = yaml.safe_load(Path(os.path.join(root_path,"data_config.yaml")).read_text())

# read config file
conf = yaml.safe_load(Path(os.path.join(root_path,"config.yaml")).read_text())

# obtain id and execute code
id = None
if "_" in role:
    id = role.split("_")[1]
    if id:

        # select corresponding theme 
        theme = data_conf["THEMES"][id]  
        print(f"Client{id}: Selected theme is: {theme}.")        
        
        # create destination folder to save data to analyze.
        data_path = os.path.join(root_path, data_conf["DATA_PATH"])
        os.makedirs(data_path, exist_ok=True)
 
        # create folder to save preprocess data
        main_folder = "all-rnr-annotated-threads"
        theme_path = os.path.join(os.path.join(data_path, main_folder),theme)
        out_folder = os.path.join(theme_path, "preprocess")
        os.makedirs(out_folder, exist_ok=True)

        # create json save folder
        json_out_folder = os.path.join(out_folder, "jsons")
        os.makedirs(json_out_folder, exist_ok=True)

        # create graph save folder
        graph_folder = os.path.join(out_folder, "graph")
        os.makedirs(graph_folder, exist_ok=True)

        # download and prepare data
        print(f"Client{id}: Download and prepare data for analysis.")
        dpf.download_clean_preprocess_and_structure(data_conf, data_path, theme,
                                            theme_path, out_folder,json_out_folder)
        
        # create message relation graph
        print(f"Client{id}: Creating graph from structure jsons...")
        gcaf.create_and_save_graph(json_out_folder, graph_folder, False)

        # analyze graph to obtain patterns
        print(f"Client{id}: Analysing graph to obtain patterns...")       
        # recover graph
        with open(os.path.join(graph_folder, 'graph.pkl'), 'rb') as f:
               graph = pickle.load(f)

        # get information and save in a big file
        file_path = gcaf.get_msg_information(graph, data_path)

        # split and zip file in smaller data batches
        print(f"Client{id}: Preparing data to be send to the server...")

        # create send_folder to save splitted folder
        send_folder = os.path.join(data_path, "files_to_send")
        os.makedirs(send_folder, exist_ok=True)

        # split data and save zipped to be send
        dpf.split_and_zip_files(file_path, send_folder, theme)
 
        # Send splitted and zipped data to the server
        print(f"Client{id}: Sending data to the server...")
        mqttf.find_and_send_msg(conf, send_folder, f"Client_{id}")        
        print(f"Client{id}: END.")
        
       

