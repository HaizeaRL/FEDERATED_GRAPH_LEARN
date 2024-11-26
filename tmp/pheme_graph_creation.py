# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 15:20:18 2024

@author: jonma
"""

import os
import json
import networkx as nx
import pickle
import matplotlib.pyplot as plt

def plot_subgraph(graph, msg_id):
    
    # Get outgoing neighbors (neighbors the node points to)
    outgoing_neighbors = list(graph.neighbors(msg_id))

    # Get incoming neighbors (nodes that point to the node)
    incoming_neighbors = list(graph.predecessors(msg_id))

    # Combine the two lists to get all neighbors
    all_neighbors = set(outgoing_neighbors + incoming_neighbors)   

    # Create a subgraph with the message node and its direct neighbors
    subgraph = graph.subgraph([msg_id] + list(all_neighbors))

    # Get the node colors based on the 'color' attribute
    node_colors = [data['color'] for _, data in subgraph.nodes(data=True)]

    # Draw the subgraph
    nx.draw(subgraph, with_labels=True, node_color=node_colors, 
                   edge_color='gray', font_weight='bold', font_size = 6)
    plt.title(f" Msg: {msg_id} relations")
    plt.show()
    

# recursive graph creation
def add_to_graph (graph, msg_id , msg, verbose):
    
    # if not exits create message node (attrs: text, date, rumour, color)
    if msg_id not in graph:
        graph.add_node(msg_id, node_type = "msg",
                       text = msg["text"],
                       date = msg["created_at"],
                       rumour = msg["rumour"],
                       color = "lightgreen")
        if verbose:
            print(f"Message node with id:{msg_id} added.")
       
    
    # if not exist create author node (attrs, author, color)
    if msg["author"]["screen_name"] not in graph:
        graph.add_node(msg["author"]["screen_name"],
                       node_type = "author", 
                       author_id = msg["author"]["id"],
                       name = msg["author"]["name"],
                       rumour = msg["rumour"],
                       color = "skyblue")
        if verbose:
            print(f"Author node with id:{msg['author']['screen_name']} added.")
        
       
    
    # relate message and author with "posted" relation
    graph.add_edge(msg["author"]["screen_name"], msg_id, relation="posted")
    
    if verbose:
        print(f"Posted relation between: {msg_id} - {msg['author']['screen_name']} added.")
   
    
    # Add mentions as edges
    for mention in msg.get("mentions", []):
        
        # add new mention if not exist to graph. author node (attrs, author, color)
        if mention["screen_name"] not in graph:
            
            graph.add_node(mention["screen_name"],
                           node_type = "author",  
                           author_id = mention["id"] ,
                           name = mention["name"],
                           rumour = msg["rumour"],
                           color = "skyblue")
            if verbose:
                print(f"New mentioned author added: {mention['screen_name']} added.")
            
            
        # create mention type relation
        graph.add_edge(msg_id, mention["screen_name"] ,relation="mention")   
        
        if verbose:
            print(f"Mention relation between: {msg_id} -> {mention['screen_name']} added.")        
        
       
        
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
            #plot_subgraph(graph, msg_id)
            #input()
            
 
# Save the graph to a file
with open(os.path.join(graph_folder,'graph.pkl'), 'wb') as f:
    pickle.dump(graph, f)   
    
