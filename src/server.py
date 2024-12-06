import sys
import os
from pathlib import Path
import yaml
import time
import pandas as pd
from sklearn.model_selection import train_test_split

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the module
from modules import mqtt_functions as mqttf
from modules import random_forest_train_functions as rftf

# READ DATA FROM CONFIG.YAML
root_path = "/usr/local/app/"
conf = yaml.safe_load(Path(os.path.join(root_path,"config.yaml")).read_text())

# CREATE CLIENT AND CONNECT TO PUBLIC BROKER
mqttc = mqttf.create_mqtt_client(conf)

# Record the start time
start_time = time.time()

# SUSCRIBE AND START LISTENING MESSAGES FOR A TIME
print("Server: waiting for messages...")
while time.time() - start_time < conf["TIME_LISTENING_MESSAGES"]:
    mqttc.loop_start()
    mqttc.subscribe(conf["MQTT_TOPIC"])
    mqttc.on_message = mqttf.on_message
    mqttc.loop_stop()

# After the loop ends, you can manage receiving messages here
print("Server: Time's up! Starting to manage receiving messages...")

# Join all data in a single datafarme
print("Server: Joining all data into single file ...")
save_path = os.path.join(root_path,"received_data")
file_path = rftf.join_all_data(save_path)

print("Server: Selecting columns and transforming to train the model...")

# recover data, select data and transform
df = pd.read_csv(file_path) 
selected_columns = ['msg_hour', 'propagate_to_msg', 'has_mentions', 'mentions','is_reply_message',
           'retweets', 'favourites', 'has_link', 'has_hashtag', 'emotion', 'is_rumour']
df = rftf.select_and_transform_data(df, selected_columns) # transform data

print("Server: Split data into taining and test...")

# Split the data into features (X) and target (y)
X = df.drop('is_rumour', axis=1)
y = df['is_rumour']

# Train-test split (80% training, 20% testing)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print("Server: Tune random forest model and obtain best model and parameters...")
best_model, best_params = rftf.tune_random_forest(X_train, y_train)

print("Server: Print best model evaluation...")
# print evaluation results of prediction: confusion matrix, classification report and accuracy score
y_pred = best_model.predict(X_test)
rftf.print_model_evaluation(y_test, y_pred)

print("Server: Print best model feature importance...")
rftf.print_feature_importance(best_model, X)

# Print tree and rules if correspond
if conf["VISUALIZE_TREE"] == 1:
    print("Server: Plot best model tree and show its rules...")
    train_columns = X_train.columns 
    rftf.visualize_random_forest(best_model, feature_names= train_columns)




