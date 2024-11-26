# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 11:46:33 2024

@author: jonma
"""

import os
import pickle
import networkx as nx
import matplotlib.pyplot as plt

root_path = "C:/DATA_SCIENCE_HAIZEA/tmp/all-rnr-annotated-threads/charliehebdo-all-rnr-threads"
preprocess_folder = os.path.join(root_path,"preprocess")
json_folder = os.path.join(preprocess_folder, "jsons")
graph_folder = os.path.join(root_path, "graph")

with open(os.path.join(graph_folder,'graph.pkl'), 'rb') as f:
    g = pickle.load(f)    
    

def plot_subgraph(graph, msg_id, type_in_out):
    
    neighbors = None
    if type_in_out == "OUT":
        # Get outgoing neighbors (neighbors the node points to)
        neighbors = list(graph.neighbors(msg_id))
    elif type_in_out == "IN":
        # Get incoming neighbors (nodes that point to the node)
        neighbors = list(graph.predecessors(msg_id))


    # Create a subgraph with the message node and its direct neighbors
    subgraph = graph.subgraph([msg_id] + list(neighbors))

    # Get the node colors based on the 'color' attribute
    node_colors = [data['color'] for _, data in subgraph.nodes(data=True)]

    # Draw the subgraph
    nx.draw(subgraph, with_labels=True, node_color=node_colors, 
                   edge_color='gray', font_weight='bold', font_size = 6)
    plt.title(f" Msg: {msg_id} relations")
    plt.show()
    

rumour_msg = [node for node, attrs in g.nodes(data=True) if attrs.get('rumour') == True \
              and attrs.get("node_type") == "msg"]
    
rumour_author = [node for node, attrs in g.nodes(data=True) if attrs.get('rumour') == True \
              and attrs.get("node_type") == "author"]
 
# DETECTED RUMOURS MESSAGES
print("Rumours msgs:",len(rumour_msg))
    

 
# IN_DEGREE CENTRALITY: THE RUMOUR MESSAGE THAT RECEIVE MORE REPLIES
in_degree = nx.in_degree_centrality(g)
max_i_node = max(
    (node for node in rumour_msg),
    key=lambda node: in_degree[node]
)

print(f"The message {max_i_node} with text:{g.nodes[max_i_node]['text']} receive most replies")
print(f"With the degree of: {in_degree[max_i_node]}")
plot_subgraph(g ,max_i_node, "IN")      
      
# OUT_DEGREE CENTRALITY: THE RUMOUR THAT PROPAGATES MOST
out_degree = nx.out_degree_centrality(g)
max_o_node = max(
    (node for node in rumour_msg),
    key=lambda node: out_degree[node]
)

print(f"The message {max_o_node} with text:{g.nodes[max_o_node]['text']} is that most propagate")
print(f"With the degree of: {out_degree[max_o_node]}")
plot_subgraph(g ,max_o_node , "OUT")    
        

      
# OUT_DEGREE CENTRALITY: THE AUTHOR THAT PROPAGATES RUMOURS MOST
out_degree_author = nx.out_degree_centrality(g)
max_o_node_author = max(
    (author for author in rumour_author),
    key=lambda author: out_degree_author[author]
)

print(f"The author {max_o_node_author} with name :{g.nodes[max_o_node_author]['name']} is that most rumours post.")
print(f"With the degree of: {out_degree_author[max_o_node_author]}")
plot_subgraph(g ,max_o_node_author , "OUT")     

 