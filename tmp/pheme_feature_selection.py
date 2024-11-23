# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 16:30:17 2024
Obtain valuable data from each msg thread
@author: hrumayor
"""

'''
Data for each message
id: Unique identifier of the message.
is_rumour: boolean whether is rumour or not.
text: Message text
in_reply_to_status_id: Relations between messages. (edges).
user.screen_name: Author messages (users node).
entities.user_mentions: id y name only. Relation between mentioned users (edges & nodes).
retweet_count & favorite_count: For node and relation ponderation.
created_at: For temporal analysis of diffusion.
'''


import os
import json
import pandas as pd
from datetime import datetime

pd.set_option('display.max_columns', None)

def fomat_date (date_str):
    # Convert to a datetime object
    dt = datetime.strptime(date_str, "%a %b %d %H:%M:%S %z %Y")

    # Format to a more readable value
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def extract_data_and_add_to_table (df, data, msg_type):
    # Create a new row dictionary
    new_row = {
        "id": str(data["id"]),
        "is_rumour": msg_type == "rumours",
        "text": data["text"],        
        "in_reply_to_id":str(data["in_reply_to_status_id"]),
        "author": data["user"]["screen_name"],    
        "retweet_count": data["retweet_count"],
        "favorite_count": data["favorite_count"],
        "created_at": fomat_date(data["created_at"]),
        "mentions": [{'id': str(mention['id']), 'name': mention['name']} for mention in data["entities"]['user_mentions']]
    }
    # Add the new row to the DataFrame
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    return df
    
def obtain_json_folders(path):
    # from all files only directories 
    return [item for item in os.listdir(path) if os.path.isdir(os.path.join(path, item))]  
 
   
def obtain_json_list(folder_path):    
    jsons = os.listdir(folder_path)  
    return jsons
    
   
def extract_type_and_threadId_from_path(path):
    # normalize path
    normalized_path = os.path.normpath(path)

    # split path parts
    path_parts = normalized_path.split(os.sep)

    # return smg type: rumours or non-rumours and table name
    return path_parts[-2], path_parts[-1]

      
def preprocess_data(path, save_path):
    """
    Recursively traverse the directory, process JSON files, and save summaries.

    Parameters:
    - path (str): Current directory path to process.
    - save_path (str): Directory to save the processed parquet files.
    """
    folder_contents = os.listdir(path)  # List all items in the current directory

    for item in folder_contents:
        item_path = os.path.join(path, item)  # Full path of the item
        
        if os.path.isdir(item_path):  # If it's a directory
            # Check if it contains `.json` files directly or process further
            if any(file.endswith(".json") for file in os.listdir(item_path)):
                # Process this folder containing JSON files
                msg_type, df_name = extract_type_and_threadId_from_path(item_path)
                folders = obtain_json_folders(item_path)

                # Initialize an empty DataFrame with defined columns
                columns = ["id", "is_rumour", "text",  "in_reply_to_id", 
                           "author", "retweet_count", "favorite_count", 
                           "created_at", "mentions"]
                df = pd.DataFrame(columns=columns)
                                                
                # loop subfolders: reactions and source-tweets
                for folder in folders:
                    folder_path = os.path.join(item_path, folder)
                    jsons = obtain_json_list(folder_path)  # List JSON files
                    # loop json
                    for js in jsons:
                        with open(os.path.join(folder_path, js), 'r') as file:
                            data = json.load(file)  # Load JSON data
                            # Add JSON data to the DataFrame
                            df = extract_data_and_add_to_table(df, data, msg_type)
                
                # Save the DataFrame after processing the folder
                filename = f"{df_name}.parquet"
                full_path = os.path.join(save_path, filename)
                df.to_parquet(full_path, engine="pyarrow")
                print("File:", filename, "has shape:", df.shape)

            else:
                # Recurse into subdirectories
                preprocess_data(item_path,save_path)

# Example Execution
root_path = "C:/DATA_SCIENCE_HAIZEA/tmp/all-rnr-annotated-threads/charliehebdo-all-rnr-threads"
out_folder = os.path.join(root_path, "preprocess")
os.makedirs(out_folder, exist_ok=True)

# Start processing
preprocess_data(root_path, out_folder)