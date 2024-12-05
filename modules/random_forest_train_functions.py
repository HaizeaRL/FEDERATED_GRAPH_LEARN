import os 
import pandas as pd
import zipfile

def join_all_data(path):
    
    # Create an empty DataFrame to store all data
    master_df = pd.DataFrame()

    # Iterate through each zip file in the directory
    for file in os.listdir(path):
        if file.endswith(".zip"):  # Process only .zip files
            path_to_zip_file = os.path.join(path, file)
            # Extract the zip file
            with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
                zip_ref.extractall(path)
            
            # Derive the corresponding CSV filename
            filename = file.split(".zip")[0] + ".csv"
            csv_path = os.path.join(path, filename)
            
            # Check if the CSV file exists and read its content
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path)
                # Append to the master dataframe
                master_df = pd.concat([master_df, df], ignore_index=True)

    # create new folder to save the result
    preprocess_folder = os.path.join(path,"preprocess")
    os.makedirs(preprocess_folder, exist_ok=True)
    
    # create filename
    file_path = os.path.join(preprocess_folder,"combined_data.csv")
    master_df.to_csv(file_path, index=False)
    
    # returns the file path
    return file_path

