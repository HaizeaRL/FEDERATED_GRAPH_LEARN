# GRAPH RELATED FUNCTIONS
import os
import json
import networkx as nx
import pickle

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

def create_and_save_graph(json_folder, graph_folder, verbose):
    # Create a directed graph
    graph = nx.DiGraph()

    # read each sctructure json and add to graph
    for file in os.listdir(json_folder):        
        if verbose:
            print("FILE:", file)            
        # open each json file
        with open(os.path.join(json_folder,file), 'r') as file:
            data = json.load(file)
            
        # read msg jsons
        if data:       
            for msg_id, msg in data.items():    
                # create messge relations
                add_to_graph (graph, msg_id , msg, verbose)   
                
    
        # Save the graph to a file
        with open(os.path.join(graph_folder,'graph.pkl'), 'wb') as f:
            pickle.dump(graph, f)   