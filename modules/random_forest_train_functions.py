import os 
import pandas as pd
import zipfile
import numpy as np
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from sklearn.tree import export_text, plot_tree
import matplotlib.pyplot as plt


def join_all_data(path):
    """
    Join all data from multiple .zip files containing CSVs into a single DataFrame and save the combined data.

    Parameters:
        path: The directory path containing the .zip files with CSVs to be processed.

    Output:
        Returns the file path of the combined CSV file saved after processing all .zip files.
    """    
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

def transform_booleans(df):
    """
    Convert boolean columns in the DataFrame to integer values (0 or 1).

    Parameters:
        df: The input DataFrame containing the dataset with boolean columns.

    Output:
        Returns the DataFrame with boolean columns converted to integers (True becomes 1, False becomes 0).
    """
    bool_cols = df.select_dtypes(include='bool').columns
    df[bool_cols] = np.multiply(df[bool_cols], 1)
    return df

def transform_strings(df):
    """
    Transform string columns in the DataFrame to numeric using Label Encoding.

    Parameters:
        df: The input DataFrame containing the dataset with string (object) columns.

    Output:
        Returns the DataFrame with string columns transformed into numeric values using Label Encoding.
    """
    str_cols = df.select_dtypes(include='object').columns
    le = preprocessing.LabelEncoder()
    for col in str_cols:
        le.fit(df[col])
        df[col] = le.transform(df[col])
    return df

def select_and_transform_data (df, desired_columns):
    """
    Select specific columns from the dataset and apply transformations to prepare the data for model training.

    Parameters:
        df: The input DataFrame containing the dataset.
        desired_columns: A list of column names to be selected from the DataFrame.

    Output:
        Returns a DataFrame with the selected columns, where boolean values are converted to integers and string values are transformed to numeric.
    """  
    # filter columns
    df = df[desired_columns]

    # convert booleans to int
    df = transform_booleans(df)

    # convert string to in
    df = transform_strings(df)

    return df

def tune_random_forest(X_train, y_train):
    """
    Tune hyperparameters for a Random Forest Classifier using GridSearchCV.

    Parameters:
        X_train: Training features
        y_train: Training labels

    Returns:
        best_model: The best Random Forest model from Grid Search
        best_params: The best hyperparameters
    """
    # Define the parameter grid
    param_grid = {
        'n_estimators': [100, 200, 300],
        'max_depth': [None, 10, 20, 30],
        'min_samples_split': [2, 5, 10],
        'min_samples_leaf': [1, 2, 4],
        'max_features': ['sqrt', 'log2', None]
    }

    # Initialize the Random Forest model
    rf = RandomForestClassifier(random_state=42)

    # Perform Grid Search
    grid_search = GridSearchCV(estimator=rf, param_grid=param_grid, 
                               cv=5, n_jobs=-1, verbose=2, scoring='accuracy')

    # Fit to training data
    grid_search.fit(X_train, y_train)

    # Return best parameters and model
    best_params = grid_search.best_params_
    best_model = grid_search.best_estimator_
    return best_model, best_params

def print_model_evaluation(y_test, y_pred):
    """
    Evaluate and print the performance of the model using confusion matrix, classification report, and accuracy score.

    Parameters:
        y_test: The true labels from the test dataset.
        y_pred: The predicted labels from the model for the test dataset.

    Output:
        Prints the confusion matrix, classification report, and accuracy score to evaluate the model's performance.
    """

    print("\t1-Confusion Matrix:")
    print(confusion_matrix(y_test, y_pred))

    print("\n\t2-Classification Report:")
    print(classification_report(y_test, y_pred))

    print("\n\t3-Accuracy Score:")
    print(accuracy_score(y_test, y_pred))

def print_feature_importance(best_model, X):
    """
    Visualize the feature importance from the best Random Forest model.

    Parameters:
        best_model: The trained Random Forest model whose feature importances are to be extracted.
        X: The feature dataset used for training the model, which provides the column names.

    Output:
        Prints a table displaying the features and their corresponding importance scores, sorted in descending order.
    """
    importance = best_model.feature_importances_
    feature_importance = pd.DataFrame({'Feature': X.columns, 'Importance': importance})
    print("\n\tFeature Importance:")
    print(feature_importance.sort_values(by='Importance', ascending=False))

def visualize_random_forest(best_model, feature_names):
    """
    Visualize a decision tree from the best Random Forest model.

    Parameters:
        best_model: The best Random Forest model from Grid Search
        feature_names: List of feature names used in the training data
    """
    # Extract one tree from the Random Forest
    tree = best_model.estimators_[0]  # Use the first tree
    
    # Plot the decision tree
    plt.figure(figsize=(20, 10))
    plot_tree(tree, feature_names=feature_names, filled=True, rounded=True, fontsize=10)
    plt.title("Decision Tree from Random Forest")
    plt.show()
    
    # Print the rules of the decision tree
    tree_rules = export_text(tree, feature_names=feature_names)
    print(tree_rules)