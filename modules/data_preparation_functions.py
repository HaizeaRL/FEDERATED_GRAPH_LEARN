import tarfile
import os
import json
import pandas as pd
from datetime import datetime
import re


# TODO: GET FILES TO DOWNLOAD FROM DATA_CONFIG FILE: DOWNLOAD_DATA_PATH
# TODO: EXTRACT AND SAVE DESIRED DATA IN PROJECT: DATA_PATH
def untar_specific_theme_data (tar_file_path, theme):
    """
    Function that untar only the corresponding theme data.
    
    Parameters:
        tar_file_path (str): where to find data tar file and where to download the data.
        theme (str): subfolder to untar. This name corresponds to client id position theme.

    Returns:
       None: untar specified folder in specified folder.
    """  

    # complete folder to stract
    folder_to_extract = f"all-rnr-annotated-threads/{theme}"

    # determine destination directory. Same as the tar file's directory
    destination_directory = os.path.dirname(tar_file_path)

    # open, find and extract corresponding theme data
    with tarfile.open(tar_file_path, "r") as tar:
        # Loop through all members in the tar file
        for member in tar.getmembers():
            # Check if the member starts with desired folder
            if member.name.startswith(folder_to_extract):
                # Extract it to the destination directory
                tar.extract(member, path=destination_directory)     

def detect_removal_files(folder_list):
    """
    Function that find files that must be removed.
    
    Parameters:
        folder_list (list(str)): list of folders to analyze

    Returns:
       list: return list of files to be removed from specified folder
    """  
    start_list = [".", "._", "._"]
    return [i for i in folder_list if i.startswith(tuple(start_list))]

def remove_files (path, files_list):
    """
    Function that removes files
    
    Parameters:
        path (str): path to find files to remove.
        files_list (list(str)): list of files to be removed from specified folder

    Returns:
       None: removes specified files from specific folder.
    """  
    if len(files_list) > 0:
        for file in files_list:
            print("Removing file:", file)
            os.remove(os.path.join(path,file))  
    

# Recursive function to clean data in all directories
def clean_directory_recursively(path):
    """
    Function that recursively check subdirectories detect files to clean and removed.
    
    Parameters:
        path (str): path to find files to be removed.

    Returns:
       None: removes specified files if exist.
    """      
    # Detect and remove undesirable files in the current directory if exists
    print(f"\tChecking directory: {path}") 
    folder_contents = os.listdir(path)  
    removal_list = detect_removal_files(folder_contents)
    if len(removal_list)>0:
        print("\tRemoval files exist:", removal_list, " removing...")
        remove_files(path, removal_list)

        # Filter out removed files from the folder list
        folder_contents = [x for x in folder_contents if x not in removal_list]
        
        # Recursively process subdirectories
        for item in folder_contents:
            item_path = os.path.join(path, item)
            if os.path.isdir(item_path):  # If it's a directory, process it recursively
                clean_directory_recursively(item_path)
    else:
        print("\tThere is no files to be removed.")

def fomat_date (date_str):
    """
    Function that format the date to more readable form. 
    Date is transform to "%Y-%m-%d %H:%M:%S" format.
    
    Parameters:
        date_str (str): date string

    Returns:
       Formats date to specified 
    """           
    # Convert to a datetime object
    dt = datetime.strptime(date_str, "%a %b %d %H:%M:%S %z %Y")

    # Format to a more readable value
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def remove_screen_names(text, screen_names):
    """
    Function that removes users mentions from messages (preceded by @) from the text.
    
    Parameters:
        text (str): text where find users mentions.
        screen_names: screen names or aliases to find in the text

    Returns:
       str: Returns new text without other users mentions.
    """      
    # Remove all screen names 
    for screen_name in screen_names:
        text = re.sub(r'@' + re.escape(screen_name), '', text)
    return text

def clean_text (data): 
    """
    Function that determines whether the message has mentions and remove
    those mentions from the text
    
    Parameters:
        data: Message structure in json format.

    Returns:
       mentions: List of mentions author. Extracts id, name and screen name from them.
       cleaned_text: If the message has mentions, remove its mentions from the message text.
    """        
    # determine if exists mentions in the message
    mentions = [{'id': str(mention['id']), 'name': mention['name'],
                  'screen_name': mention['screen_name']} for mention in data["entities"]['user_mentions']]
     
    # remove author references from text:
    if len(mentions)>0:
        # Extract the screen names from the mentions dictionary
        screen_names = [mention['screen_name'] for mention in mentions]
        
        # Cleaned text
        cleaned_text = remove_screen_names(data["text"], screen_names)
        
        # Clean up extra spaces from the text
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
    else:
        cleaned_text = data["text"]
        
    return mentions, cleaned_text
 

def extract_data_and_add_to_table (df, data, msg_type):
    """
    Function that add to summary dataframe extracted features.
    From each message extracts:
        -id: Unique identifier of the message.
        -is_rumour: boolean whether is rumour or not.
        -text: Message text wihtout mentions in it.
        -in_reply_to_status_id: Relations between messages. (edges).
        -user.screen_name, user.id and user.name: Author data (users node).
        -entities.user_mentions: id y name only. Relation between mentioned users (edges & nodes).
        -retweet_count & favorite_count: For node and relation ponderation.
        -created_at: For temporal analysis of diffusion.
    
    Parameters:
        df: summary table with all messages data.
        data: Message to be analized structure in json format.
        msg_type: Text whether indicates the message is "rumour" or not.

    Returns:
       df: New row is added to summary df.
    """        
    # Remove from text other authors references
    mentions, cleaned_text = clean_text (data)       
            
    # Create a new row dictionary
    new_row = {
        "id": str(data["id"]),
        "is_rumour": msg_type == "rumours",
        "text": cleaned_text,        
        "in_reply_to_id":str(data["in_reply_to_status_id"]),
        "author": {"id": str(data["user"]["id"]), "name": data["user"]["name"],
                "screen_name": data["user"]["screen_name"]
            },
        "retweet_count": data["retweet_count"],
        "favorite_count": data["favorite_count"],
        "created_at": fomat_date(data["created_at"]),
        "mentions": mentions
        
    }
    # Add the new row to the DataFrame
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    return df
    
def obtain_json_folders(path):
    """
    Function that finds only folders where jsons are in it.
    
    Parameters:
        path (str): Path to check.   

    Returns:
      return list of forlders where msg json are in it.
    """        
    # from all files only directories 
    return [item for item in os.listdir(path) if os.path.isdir(os.path.join(path, item))]  
 
   
def obtain_json_list(folder_path):  
    """
    Function that obtains json list from a folder.
    
    Parameters:
        folder_path (str): Path to check.   

    Returns:
      return list of json in a specific folder.
    """          
    jsons = os.listdir(folder_path)  
    return jsons
    
   
def extract_type_and_threadId_from_path(path):
    """
    Function that extracts from folder folder path.     
    Parameters:
        path (str): Path to extract the mentioned data.

    Returns:
      Whether the message is rumour or not.
      And the theme or the group where belongs the messages.
    """          
    # normalize path
    normalized_path = os.path.normpath(path)

    # split path parts
    path_parts = normalized_path.split(os.sep)

    # return smg type: rumours or non-rumours and table name
    return path_parts[-2], path_parts[-1]

      
def preprocess_data(path, save_path, verbose):
    """
    Recursively traverse the directory, process JSON files, and save summaries.
    Data is saved in parquet files.

    Parameters:
      path (str): Current directory path to process.
      save_path (str): Directory to save the processed parquet files.
      verbose (boolean): whether verbose log must be logged or not.
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
                if verbose:
                    print("\tFile:", filename, "has shape:", df.shape)
            else:
                # Recurse into subdirectories
                preprocess_data(item_path, save_path, verbose)
    print("\tData extraction finished.")

   
    
    
   






