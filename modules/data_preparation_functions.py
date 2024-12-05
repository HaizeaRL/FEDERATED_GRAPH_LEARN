import os
import wget
from pathlib import Path
import zipfile
import tarfile
import json
import chardet
import pandas as pd
from datetime import datetime
import re

def untar_specific_theme_data (file_url, tar_file_path, theme):
    """
    Function that download data to the analysis from webpage,
    and get only the corresponding theme data.
    
    Parameters:
        file_url (str): Url to download the files.
        tar_file_path (str): where to find data tar file and where to download the data.
        theme (str): subfolder to untar. This name corresponds to client id position theme.

    Returns:
       None: untar specified folder in specified folder.
    """  
   
    # determine destination directory. Same as the tar file's directory
    destination_directory = tar_file_path

    # get and download zip and save in specified destination path  
    print(f"\tDownloading data from  {file_url}...") 
    wget.download(file_url, destination_directory) 

    # unzip zip file
    zip_file_path = os.path.join(destination_directory, "6392078.zip")
    print(f"\n\tUnziping downloaded file...")
    with zipfile.ZipFile(zip_file_path, "r") as zip_file:       
        zip_file.extractall(destination_directory)

    # remove after unzip
    os.remove(zip_file_path) 

    # Untar tar.gz file
    tar_gz_file_path = os.path.join(destination_directory, "PHEME_veracity.tar.bz2")
    folder_to_extract = f"all-rnr-annotated-threads/{theme}"
    with tarfile.open(tar_gz_file_path, "r:gz") as tar:
        # Loop through all members in the tar file
        for member in tar.getmembers():
            # Check if the member starts with desired folder
            if member.name.startswith(folder_to_extract):
                # Extract it to the destination directory
                tar.extract(member, path=destination_directory)
    
    # remove after untar tar.gz file
    os.remove(tar_gz_file_path) 
  

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

def remove_files (path, files_list, verbose):
    """
    Function that removes files
    
    Parameters:
        path (str): path to find files to remove.
        files_list (list(str)): list of files to be removed from specified folder
        verbose (boolean): whether log need to be prompt or not.

    Returns:
       None: removes specified files from specific folder.
    """  
    if len(files_list) > 0:
        for file in files_list:
            if verbose:
                print("Removing file:", file)            
            os.remove(os.path.join(path,file))  
    

# Recursive function to clean data in all directories
def clean_directory_recursively(path, verbose):
    """
    Function that recursively check subdirectories detect files to clean and removed.
    
    Parameters:
        path (str): path to find files to be removed.
        verbose (boolean): whether log need to be prompt or not.

    Returns:
       None: removes specified files if exist.
    """      
    # Detect and remove undesirable files in the current directory if exists
    if verbose:
        print(f"\tChecking directory: {path}")    
    folder_contents = os.listdir(path)  
    removal_list = detect_removal_files(folder_contents)
    
    if verbose:
        print("\tRemoval files exist:", removal_list, " removing...")
    remove_files(path, removal_list, verbose)

    # Filter out removed files from the folder list
    folder_contents = [x for x in folder_contents if x not in removal_list]
    
    # Recursively process subdirectories
    for item in folder_contents:
        item_path = os.path.join(path, item)
        if os.path.isdir(item_path):  # If it's a directory, process it recursively
            clean_directory_recursively(item_path, verbose)
   

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
    
def save_as_json(data, file_path , filename, verbose):
    """
    Save the given data to a JSON file.

    Parameters:
      data (dict): The structured data to save.
      file_path (str): The file path where the JSON file should be saved.
      verbose (boolean): whether verbose log must be logged or not.

    Returns:
        Saves json as json file in specified path.
    """
    # Convert the data to a JSON serializable format
    json_data = json.dumps(data, indent=4)
    
    # Open the file in write mode and save the JSON data
    with open(file_path, 'w', encoding='utf-8') as json_file:
        json_file.write(json_data)

    if verbose:
        print(f"\t{filename} succesfully saved.")

def build_reply_structure(messages, df, message_id): 
    """
    Recursively builds a hierarchical structure for a message and its replies.

    Parameters:
        messages (dict): A dictionary where the keys are message IDs and the values are message data.
                         Each message's data is expected to be a dictionary with attributes such as 
                         'author', 'is_rumour', 'text', etc.
        df (pandas.DataFrame): A DataFrame containing all messages, with at least the columns 
                               'id' and 'in_reply_to_id'.
        message_id (str): The ID of the message for which to build the structure.

    Returns:
        dict: A nested dictionary representing the message and its replies. 
              Each dictionary contains:
              - 'author': The author of the message.
              - 'rumour': A boolean indicating whether the message is a rumour.
              - 'text': The text content of the message.
              - 'retweet_count': The number of times the message has been retweeted.
              - 'favorite_count': The number of times the message has been favorited.
              - 'created_at': The timestamp when the message was created.
              - 'mentions': A list of mentions in the message.
              - 'replies': A nested dictionary of replies, recursively structured in the same way.
    """
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
   """
    Converts message data from Parquet files into a structured JSON format, 
    organizing each message and its replies hierarchically.

    Parameters:
        prep_folder (str): Path to the folder containing the Parquet files. 
                           Each file represents messages for a specific root message.
        save_folder (str): Path to the folder where the structured JSON files will be saved.

    Returns:
        None: This function saves the structured data as JSON files in the specified save folder.
    """   
   for file in os.listdir(prep_folder):       
       if file.endswith(".parquet"):
           # open table
           df = pd.read_parquet(os.path.join(prep_folder, file), engine="pyarrow")
            
           # Build a dictionary of all messages indexed by their id
           messages = {row['id']: row.drop('id') for _, row in df.iterrows()}
            
           # create json
           root_message_id  = file.split(".parquet")[0]
           structured_data = {root_message_id: build_reply_structure(messages, df, root_message_id)}
            
           # save json
           file_path = os.path.join(save_folder,f"{root_message_id}.json")
           save_as_json(structured_data, file_path, f"{root_message_id}.json", False)   
    
            
def download_clean_preprocess_and_structure(data_conf, data_path, theme,
                                            theme_path, out_folder,json_out_folder):
    """
    Function that handles the complete pipeline for downloading, cleaning, preprocessing, and 
    structuring message data into hierarchical JSON structures.

    Parameters:
        data_conf (dict): Configuration for downloading data, containing the key "download_data_url" 
                          for the URL of the data to be downloaded.
        data_path (str): Path where the downloaded data will be extracted.
        theme (str): The theme or topic for which the data is being processed.
        theme_path (str): Path to the folder containing theme-specific data for preprocessing.
        out_folder (str): Path to the folder where cleaned and processed data will be saved.
        json_out_folder (str): Path to the folder where the structured JSON files will be saved.

    Returns:
        None: The function performs processing and saves outputs to the specified directories.
    """

    # unzip corresponding data
    untar_specific_theme_data(data_conf["DOWNLOAD_DATA_URL"], data_path, theme)
    print(f"\tData downloaded: Folder: {theme} and its subfolders extracted to {data_path}. Preparing...")
            
    # prepare data for analysis
    print("\tStep 1: Cleaning...")

    # 1- clean not desirable files if exists. Recursively        
    root_path = os.path.join(data_path,"all-rnr-annotated-threads") # Entry point
    clean_directory_recursively(root_path, False)
    
    # 2- Feature selection. Recursively  
    print(f"\tStep 2: Selecting and extracting features from messages ...")

    # Start data selection
    preprocess_data(theme_path, out_folder, False)
    print(f"\tStep 3: Creating message structure jsons...")

    # 3- Msg structure relation json creation. Recursively.
    complete_structure_json(out_folder, json_out_folder)
   
def split_and_zip_files (file_path, save_path, theme):
    """
    Function that chunks data into smaller pieces and zips each chunk to be sent.

    Parameters:      
      file_path (str): Path where df filename is saved.
      save_path (str): Directory to save the zipped files.
      theme (str): Theme where data belongs to use as filename.

    Returns:
      The files are saved in zip form.
    """

    # recover saved data
    df = pd.read_parquet(file_path, engine = "pyarrow")
    
    # create file chunks
    chunks = [df.iloc[i:i + 100] for i in range(0, len(df), 100)]
    
    # zip file chunks
    for idx, chunk in enumerate(chunks):
        # compose filenames
        file_name = f'{theme.split("-")[0]}_{idx}.csv'
        zip_name = f'{theme.split("-")[0]}_{idx}.zip'

        # Save each chunk as CSV file
        chunk.to_csv(os.path.join(save_path,file_name), index=False) 

        # Compress each file into its own ZIP archive
        with zipfile.ZipFile(os.path.join(save_path,zip_name), 'w') as zipf:
            zipf.write(os.path.join(save_path,file_name), arcname=file_name) # csv file without path

        # remove csv file
        os.remove(os.path.join(save_path,file_name)) 





