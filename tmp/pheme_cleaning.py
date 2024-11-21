# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 15:39:43 2024
Clean not desirable files from pheme folder.
@author: hrumayor
"""

import os

def detect_removal_files(folder_list):
    start_list = [".", "._", "._"]
    return [i for i in folder_list if i.startswith(tuple(start_list))]

def remove_files (path, files_list):
    if len(files_list) > 0:
        for file in files_list:
            print("Removing file:", file)
            os.remove(os.path.join(path,file))  
    

# Recursive function to clean data in all directories
def clean_directory_recursively(path):
    print(f"Checking directory: {path}")      
    folder_contents = os.listdir(path)   
    # Detect and remove undesirable files in the current directory
    removal_list = detect_removal_files(folder_contents)
    print("Removal files exist:", removal_list)
    remove_files(path, removal_list)
    
    # Filter out removed files from the folder list
    folder_contents = [x for x in folder_contents if x not in removal_list]
    
    # Recursively process subdirectories
    for item in folder_contents:
        item_path = os.path.join(path, item)
        if os.path.isdir(item_path):  # If it's a directory, process it recursively
            clean_directory_recursively(item_path)

# Entry point
root_path = "C:/DATA_SCIENCE_HAIZEA/tmp/all-rnr-annotated-threads"
clean_directory_recursively(root_path)