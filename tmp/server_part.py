# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 15:29:35 2024

@author: hrumayor
"""

# open files and join in a single dataframe
import os
import pandas as pd
import zipfile

pd.set_option("display.max_columns", None)


path ="C:/DATA_SCIENCE_HAIZEA/tmp1/received_data"


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
    

def select_and_transform_data(df):
    
    # data to select
    columns = ['msg_hour', 'propagate_to_msg', 'has_mentions', 'is_reply_message',
               'retweets', 'favourites', 'has_link', 'has_hashtag', 'emotion', 'is_rumour']
    df = df[columns]
    
    # transform data 
    # select boolean type data:
    bool_cols = df.select_dtypes(include='bool')
    df[bool_cols] = df[bool_cols].astype(int)
    
    # Step 2: One-hot encode the str type columns column
    str_cols = df.select_dtypes(include='object')
    df = pd.get_dummies(df, columns=[str_cols], drop_first=True)
    
    
    

# Join all data and save in new folder
file_path = join_all_data(path)

# open the file
df = pd.read_csv(file_path)

df.info()

# select analysis_data
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import numpy as np
from sklearn.tree import export_graphviz, plot_tree
import matplotlib.pyplot as plt

# Assuming `df` is your dataframe
# Ensure the dataset contains only relevant columns
# data to select
columns = ['msg_hour', 'propagate_to_msg', 'has_mentions', 'is_reply_message',
           'retweets', 'favourites', 'has_link', 'has_hashtag', 'emotion', 'is_rumour']
df = df[columns]

# transform data 


# Step 2: One-hot encode the str type columns column
str_cols = df.select_dtypes(include='object').columns
df = pd.get_dummies(df, columns=str_cols)

# select boolean type data:
bool_cols = df.select_dtypes(include='bool').columns
df[bool_cols] = np.multiply(df[bool_cols], 1)
 

# Split the data into features (X) and target (y)
X = df.drop('is_rumour', axis=1)
y = df['is_rumour']

# Train-test split (80% training, 20% testing)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize the Random Forest model
rf_model = RandomForestClassifier(n_estimators=100, random_state=42)

# Train the model
rf_model.fit(X_train, y_train)


# Predict on the test set
y_pred = rf_model.predict(X_test)

# Evaluate the model
print("Confusion Matrix:")
print(confusion_matrix(y_test, y_pred))
print("\nClassification Report:")
print(classification_report(y_test, y_pred))
print("\nAccuracy Score:")
print(accuracy_score(y_test, y_pred))

# Feature importance
importance = rf_model.feature_importances_
feature_importance = pd.DataFrame({'Feature': X.columns, 'Importance': importance})
print("\nFeature Importance:")
print(feature_importance.sort_values(by='Importance', ascending=False))
