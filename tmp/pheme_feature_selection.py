# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 16:30:17 2024

@author: hrumayor
"""

'''
Datos interesantes de cada mensaje:
text: Texto
id: Identificador único del mensaje, útil como nodo del grafo.
in_reply_to_status_id: Indica relaciones entre mensajes (aristas).
user.screen_name: Para identificar al autor del mensaje (nodos de usuarios).
entities.user_mentions: id y name solo. Relaciones con usuarios mencionados (aristas y nodos).
retweet_count y favorite_count: Para ponderar nodos o relaciones.
created_at: Para análisis temporal de la difusión.
'''

import json

root_path = "C:\DATA_SCIENCE_HAIZEA\tmp2\charliehebdo-all-rnr-threads\non-rumours\552784600502915072\reactions"






device_disco["device"] =[dict(username=k1["username"],password=k1["password"],ip=k1["ip"]) for k1 in 
device_disco["device"]]

jsonData = json.dumps(device_disco)
print (jsonData)