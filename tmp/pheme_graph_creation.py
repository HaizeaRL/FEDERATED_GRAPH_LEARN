# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 15:20:18 2024

@author: jonma
"""

import os
import json
import networkx as nx
import pickle

# recursive graph creation
def add_to_graph (graph, msg_id , msg, verbose):
    
    # if not exits create message node (attrs: text, date, rumour, color)
    if msg_id not in graph:
        graph.add_node(msg_id, type = "msg", text = msg["text"],
                       date = msg["created_at"],
                       rumour = msg["rumour"],
                       color = "lightgreen")
        if verbose:
            print(f"Message node with id:{msg_id} added.")
       
    
    # if not exist create author node (attrs, author, color)
    if msg["author"]["id"] not in graph:
        graph.add_node(msg["author"]["id"], type = "author", 
                       author = msg["author"]["screen_name"],
                       color = "skyblue")
        if verbose:
            print(f"Author node with id:{msg['author']['id']} added.")
        
       
    
    # relate message and author with "posted" relation
    graph.add_edge(msg["author"]["id"], msg_id, relation="posted")
    
    if verbose:
        print(f"Posted relation between: {msg_id} - {msg['author']['id']} added.")
   
    
    # Add mentions as edges
    for mention in msg.get("mentions", []):
        
        # add new mention if not exist to graph. author node (attrs, author, color)
        if mention["id"] not in graph:
            
            graph.add_node(mention["id"],type = "author", 
                           author = mention["screen_name"],
                           color = "skyblue")
            if verbose:
                print(f"New mentioned author added: {mention['id']} added.")
            
            
        # create mention type relation
        graph.add_edge(msg_id, mention["id"] ,relation="mention")   
        
        if verbose:
            print(f"Mention relation between: {msg_id} -> {mention['id']} added.")        
        
       
        
    # Recursively process replies
    for reply_id, reply in msg.get("replies", {}).items():
        
        # create msg-author nodes & relation
        add_to_graph (graph, reply_id , reply, verbose)
        
       
        # create reply type relation
        graph.add_edge(reply_id, msg_id, relation="replies", 
                       retweet = reply["retweet_count"],
                       favourite = reply["favorite_count"])
        
        if verbose:
            print(f"Replay relation between: {reply_id} -> {msg_id} added.")
     

# Example Execution
root_path = "C:/DATA_SCIENCE_HAIZEA/tmp/all-rnr-annotated-threads/charliehebdo-all-rnr-threads"
preprocess_folder = os.path.join(root_path,"preprocess")
json_folder = os.path.join(preprocess_folder, "jsons")
graph_folder = os.path.join(root_path, "graph")
os.makedirs(graph_folder, exist_ok=True)
       

# Create a directed graph
graph = nx.DiGraph()

# read each sctructure json and add to graph
for file in os.listdir(json_folder):
    
    print("FILE:", file)
        
    # open each json file
    with open(os.path.join(json_folder,file), 'r') as file:
        data = json.load(file)
        
    # read msg jsons
    if data:       
        for msg_id, msg in data.items():    
            # create messge relations
            add_to_graph (graph, msg_id , msg, False)         
            
 
# Save the graph to a file
with open(os.path.join(graph_folder,'graph.pkl'), 'wb') as f:
    pickle.dump(graph, f)   
    
with open(os.path.join(graph_folder,'graph.pkl'), 'rb') as f:
    loaded_graph = pickle.load(f)    

# NODOS SOLO DE RUMORES    
rumour_nodes = [
    node for node, attrs in loaded_graph.nodes(data=True) if attrs.get('rumour') == True
]

    
'''
LOAD GRAPH
with open('graph.pkl', 'rb') as f:
    loaded_graph = pickle.load(f)        
            

import matplotlib.pyplot as plt
# Set the node color based on node type (message or author)
node_color = []
for node in graph.nodes():
    if graph.nodes[node]['type'] == 'msg':
        node_color.append('lightgreen')  # Color for message nodes
    else:
        node_color.append('skyblue')  # Color for author nodes

# Create a layout for better visualization (spring layout)
pos = nx.spring_layout(graph, seed=42)

# Draw the graph with labels for nodes
plt.figure()
nx.draw(graph, pos, with_labels=True, node_size=500, node_color=node_color, font_size=8, font_weight='bold', arrowsize=10)

# Display the graph
plt.title("Message and Author Network")
plt.show()'''