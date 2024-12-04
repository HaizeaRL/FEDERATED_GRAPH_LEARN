import paho.mqtt.client as mqtt
import base64
import json
import os
import time

def create_mqtt_client (conf):
    """
    Function that creates mqtt client connected to public mqtt broker.
    
    Parameters:
        conf (dict): config.yaml's information

    Returns:
       mqtt client connected to public mqtt broker.
    """  

    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.connect(conf["MQTT_BROKER"], conf["MQTT_PORT"], conf["MQTT_KEEPALIVE"])
    return mqttc

def on_message (client, userdatata, message):
    """
    Function that prints received mqtt message payload   
    """  
    # Decode the MQTT message payload
    decoded_message = message.payload.decode("utf-8")
    
    # Parse the JSON data
    json_msg = json.loads(decoded_message)
    print("Received message: ",json_msg.get("filename"))

def encode_zip_to_base64(zip_file_path):
    """
    Encodes a ZIP file into a Base64 string for sending via MQTT.

    Parameters:
      zip_file_path (str): The path to the ZIP file.

    Returns:
      str: Base64-encoded string of the ZIP file.
    """
    with open(zip_file_path, 'rb') as zip_file:
        base64_string = base64.b64encode(zip_file.read()).decode('utf-8')
    return base64_string


def prepare_mqtt_message(client_id, base64_str, file_name):   
    """
    Function that prepare the message to be send by mqtt

    Parameters:
      client_id (str): Client node identification.
      base64_str (base64 string): zip file encoded in base64.
      file_name (str) : filename of the data

    Returns:
      json: Json message to be send by mqtt
    """ 
    json_msg =  {"client": client_id, "data": base64_str, "filename": file_name}
    message = json.dumps(json_msg)
    return message

def find_and_send_msg(conf, send_folder, client_id):

    # obtain mqtt client
    mqttc = create_mqtt_client(conf)

    # obtain files from send folder
    file_list = os.listdir(send_folder)  

    for i, zip_file in enumerate(file_list):

        # convert file in base64 string
        base64_str = encode_zip_to_base64(os.path.join(send_folder,zip_file))

        # obtain msg to be send from mqtt
        message = prepare_mqtt_message(client_id, base64_str, zip_file)

        # send message
        mqttc.publish(conf["MQTT_TOPIC"], message)
        print(f"\tFile {zip_file} published in {conf['MQTT_TOPIC']} topic.")
        time.sleep(120)

