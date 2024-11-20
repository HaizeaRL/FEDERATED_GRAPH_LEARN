import paho.mqtt.client as mqtt

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
    print("Received message: ",str(message.payload.decode("utf-8")))
