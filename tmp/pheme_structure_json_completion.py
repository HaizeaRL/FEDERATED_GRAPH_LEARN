# -*- coding: utf-8 -*-
"""
Created on Sat Nov 23 15:58:10 2024
Create each message thread structure json
@author: jonma
"""

import os
import json
import pandas as pd
import numpy as np

def save_as_json(data, file_path , filename):
    """
    Save the given data to a JSON file.

    Parameters:
    - data (dict): The structured data to save.
    - file_path (str): The file path where the JSON file should be saved.
    """
    # Convert the data to a JSON serializable format
    json_data = json.dumps(data, indent=4)
    
    # Open the file in write mode and save the JSON data
    with open(file_path, 'w', encoding='utf-8') as json_file:
        json_file.write(json_data)
    print(f"{filename} succesfully saved.")

def build_reply_structure(messages, df, message_id): 
    # Fetch the message from the 'messages' dictionary
    message = messages.get(message_id)    
   
    # Get replies (messages that have this message's id as their in_reply_to_id)
    replies = [row for _, row in df.iterrows() if row['in_reply_to_id'] == message_id]   
    # Recursively build the reply structure for each reply
    reply_structure = {}
    for reply in replies:
        # Build structure for each reply recursively
        reply_structure[reply['id']] = build_reply_structure(messages, df, reply['id'])
  
          
    # Return the structure for this message with all its replies
    return {
        "author": message['author'],
        "rumour": message['is_rumour'],
        "text": message['text'],
        "retweet_count": message['retweet_count'],
        "favorite_count": message['favorite_count'],
        "created_at": message['created_at'],
        "mentions": message['mentions'].tolist() ,
        "replies": reply_structure
    }

def complete_structure_json(prep_folder, save_folder):
    
   for file in os.listdir(prep_folder):
              
       # open table
       df = pd.read_parquet(os.path.join(prep_folder, file), engine="pyarrow")
       
       # Build a dictionary of all messages indexed by their id
       messages = {row['id']: row.drop('id') for _, row in df.iterrows()}
       
       # create json
       root_message_id  = file.split(".parquet")[0]
       structured_data = {root_message_id: build_reply_structure(messages, df, root_message_id)}
    
       # save json
       file_path = os.path.join(save_folder,f"{root_message_id}.json")
       save_as_json(structured_data, file_path, f"{root_message_id}.json")   
   
                
# Example Execution
root_path = "C:/DATA_SCIENCE_HAIZEA/tmp/all-rnr-annotated-threads/charliehebdo-all-rnr-threads"
prep_folder = os.path.join(root_path, "preprocess")

# create json save folder
out_folder = os.path.join(prep_folder, "jsons")
os.makedirs(out_folder, exist_ok=True)
       

# Start processing
complete_structure_json(prep_folder, out_folder)