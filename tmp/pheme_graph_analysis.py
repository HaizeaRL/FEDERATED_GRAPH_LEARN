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
    
def determine_messages_per_author(authors):
    
    author_messages = {}

    # Iterate through each author in the graph
    for author in authors:
        # Get the messages posted by the author
        posted_messages = list(g.neighbors(author))
        
        # Consider only distinct messages (unique messages)
        distinct_messages = set(posted_messages)
        
        # Store the number of distinct messages for the author
        author_messages[author] = len(distinct_messages)       


    return author_messages


def order_dictionary (in_reverse, dictionary):
    
    return {k: v for k, v in sorted(dictionary.items(), key=lambda item: item[1], reverse=in_reverse)}

    
def calculate_reverse_propagation(graph, msg_id, visited):
    """
    Recursively calculates the reverse propagation of a rumour message.
    
    :param graph: The graph.
    :param msg_id: The message node to calculate propagation for.
    :param visited: A set of visited nodes to avoid redundant traversals.
    :return: The total count of propagated messages.
    """
    if msg_id in visited:
        return 0  # Prevent infinite loops in cyclic graphs
    
    visited.add(msg_id)
    # Get only message predecessors (messages replying to this message)
    msg_predecessors = [pred for pred in graph.predecessors(msg_id) 
                        if graph.nodes[pred].get("node_type") == "msg"]
    
    #print(f"msg_id: {msg_id} and msg_predecessors:{msg_predecessors}")

    # Recursively calculate the propagation count for each predecessor
    propagation_count = 0
    for predecessor in msg_predecessors:
        propagation_count += calculate_reverse_propagation(graph, predecessor, visited)

    # Include current message's immediate predecessors in the count
    return len(msg_predecessors) + propagation_count

def calculate_mentions (messages):
    # Dictionary to store the count of mentions for each author
    mentioned_authors_count = {}

    # Iterate through messages
    for msg_id in messages:
        # Get all neighbors of the current message with a "mention" relation
        mentioned_authors = [
            neighbor for neighbor in g.neighbors(msg_id)
            if g.nodes[neighbor].get("node_type") == "author" and
               g.edges[msg_id, neighbor].get("relation") == "mention"
        ]
        
        # Count mentions for each author
        for author in mentioned_authors:
            if author not in mentioned_authors_count:
                mentioned_authors_count[author] = 0
            mentioned_authors_count[author] += 1
            
    #return result
    return mentioned_authors_count


def calculate_replies (messages):
    # Dictionary to store the count of replies of each message.
    replied_msg_data = {}

    # Iterate through messages
    for msg_id in messages:
        # Obtain reply edges to this message
        reply_edges = [
            edge for edge in g.in_edges(msg_id, data=True)
            if edge[2].get("relation") == "replies"
        ]
        
        # count and add to dictionary number of replies
        replied_msg_data[msg_id] = len(reply_edges)
        
    # return result
    return replied_msg_data

def calculate_retweets (message):
    
    # Dictionary to store retweets
    retweet_data = {}

    # Iterate through messages
    for msg_id in message:
        # Obtain reply edges to this message
        reply_edges = [
            edge for edge in g.in_edges(msg_id, data=True)
            if edge[2].get("relation") == "replies"
        ]
        # Count retweets asociate to this msg
        total_retweets = sum(edge[2].get("retweet", 0) for edge in reply_edges)
        retweet_data[msg_id] = total_retweets
    
    # return the result
    return retweet_data

def calculate_favourites (message):
    
    # Dictionary to store favourite counts
    favourite_data = {}

     # Iterate through messages
    for msg_id in rumour_msg:
        # Obtain reply edges to this message
        reply_edges = [
            edge for edge in g.in_edges(msg_id, data=True)
            if edge[2].get("relation") == "replies"
        ]
        # Count favourites asociate to this msg
        total_favourites = sum(edge[2].get("favourite", 0) for edge in reply_edges)
        favourite_data[msg_id] = total_favourites
       
    # return the result
    return favourite_data

def print_text_dict(l, msg):    
    for msg in l:
        print(msg)
        print(f"Text: {g.nodes[msg]['text']}.")
    

    
# GRAPH ANALYSIS 
# a) How many messages in the graph?
total_msg = [node for node, attrs in g.nodes(data=True) if attrs.get("node_type") == "msg"]
print(f"The graph has in total: {len(total_msg)} messages")

# b) How many rumours in the graph?
rumour_msg = [node for node, attrs in g.nodes(data=True) if attrs.get('rumour') == True \
              and attrs.get("node_type") == "msg"]

print(f"From total: {len(total_msg)} messages, {len(rumour_msg)} are rumours related ({round((len(rumour_msg) / len(total_msg)) *100,2)}%)")

# c) How many authors in the graph?
total_authors = [node for node, attrs in g.nodes(data=True) if attrs.get("node_type") == "author"]
print(f"The graph has in total: {len(total_authors)} authors")

# d) How many of them are involved with rumours

rumour_author = [node for node, attrs in g.nodes(data=True) if attrs.get('rumour') == True \
              and attrs.get("node_type") == "author"]
print(f"From total: {len(total_authors)} authors, {len(rumour_author)} are rumours related ({round((len(rumour_author) / len(total_authors)) *100,2)})%")
  

# e) Top 10 of rumours messages that most propagate

# Calculate propagation for each rumour message
propagation_dict = {}
for msg_id in rumour_msg:
    visited = set()
    propagation_dict[msg_id] = calculate_reverse_propagation(g, msg_id, visited)
    #print(f"msg:{msg_id} propagates to {propagation_dict[msg_id]} messages.")
  
   
# sort dictionary by value (in reverse form)
sorted_propagation_dict = order_dictionary (True, propagation_dict)

# select top 10 and visualize the result
top_10_propagated_msg= list(sorted_propagation_dict)[:10]
print("\nThese 10 rumour msgs are propagated more:")
for msg in top_10_propagated_msg:
    print(f"\nMessage:{msg} propagated to {propagation_dict[msg]} messages.")
    print(f"Text: {g.nodes[msg]['text']}.")
  
  

# f) Top 10 of authors that most rumours propagate
# determine rumour messages per author
message_per_author = determine_messages_per_author(rumour_author)

# sort dictionary by value (in reverse form)
sorted_message_per_author = order_dictionary (True, message_per_author)

# select top 10 and visualize the result
top_10_authors= list(sorted_message_per_author)[:10]
print("\nThese 10 authors are who most rumours propagate:")
for auth in top_10_authors:
    print(f"Author:{auth} -> {len(list(g.neighbors(auth)))} messages.")
    #plot_subgraph(g ,auth , "OUT")


# g) Top 10 authors that are most mentioned in the rumoured messages.

# calculate author mentions in rumours
mentioned_authors_count = calculate_mentions(rumour_msg)

# Sort authors by the number of mentions (descending)
sorted_mentioned_authors = order_dictionary(True, mentioned_authors_count)

# Select the top 10 authors and display their mention counts
top_10_mentioned_authors = list(sorted_mentioned_authors.items())[:10]
print("\nTop 10 authors most mentioned in rumoured messages:")
for author, count in top_10_mentioned_authors:
    print(f"Author: {author} -> {count} mentions.")


# h) Top 10 of rumours messages that most reactions or replies receive.

# calculate message replies in rumour type messages
replied_msg_data = calculate_replies (rumour_msg)

# Ordenar el diccionario por el número de respuestas en orden descendente
sorted_replied_msg_data = order_dictionary(True, replied_msg_data)

# Seleccionar el top 10
top_10_replied_msg = list(sorted_replied_msg_data)[:10]

# Mostrar los resultados
print("These 10 msgs received most reactions or replies:")
for msg in top_10_replied_msg:
    print(f"\nMessage: {msg} -> {replied_msg_data[msg]} replies")
    print(f"Text: {g.nodes[msg]['text']}")
    

# i) Top 10 of rumours messages that are the most retweeted
# calculate retweets in rumour type messages
retweet_data = calculate_retweets (rumour_msg)

# Ordenar por la cantidad de retweets en orden descendente
sorted_retweet_data = order_dictionary(True, retweet_data)

# Seleccionar el top 10
top_10_retweeted_msgs = list(sorted_retweet_data)[:10]

# Mostrar resultados
print("Top 10 messages that are most retweeted:")
for msg in top_10_retweeted_msgs:
    print(f"\nMessage: {msg} -> Retweets: {retweet_data[msg]}")
    print(f"Text: {g.nodes[msg]['text']}")


# j) Top 10 of rumours messages that are the most favourited

# calculate retweets in rumour type messages
favourite_data = calculate_favourites (rumour_msg)

# Ordenar por la cantidad de retweets en orden descendente
sorted_favourite_data = order_dictionary(True, favourite_data)

# Seleccionar el top 10
top_10_favourite_msgs = list(sorted_favourite_data)[:10]

# Mostrar resultados
print("Top 10 messages that are most favourite:")
for msg in top_10_favourite_msgs:
    print(f"\nMessage: {msg} -> Favourites: {favourite_data[msg]}")
    print(f"Text: {g.nodes[msg]['text']}")


# j) Most influent author
def calculate_author_influence(graph, authors):
    # Calculate centrality metrics for authors
    centrality = nx.betweenness_centrality(graph, normalized=True)  # Example: betweenness centrality
    
    # Get the centrality values for authors
    author_influence = {author: centrality[author] for author in authors if author in centrality}
    
    # Sort by centrality (descending order)
    sorted_author_influence = sorted(author_influence.items(), key=lambda x: x[1], reverse=True)
    
    return sorted_author_influence

# Calculate and print the influence of authors
author_influence = calculate_author_influence(g, rumour_author)

# Display top 10 most influential authors
print("Top 10 most influential authors in the network:")
for author, influence in author_influence[:10]:
    print(f"Author: {author}, Influence (Betweenness Centrality): {influence}")


# j) Period of time of most messages are send.

# k) Period of time of most rumour messages are send.

# l) Key words of rumours & emotions of those rumours.

