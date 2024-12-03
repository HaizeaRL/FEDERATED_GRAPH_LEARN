# -*- coding: utf-8 -*-
"""
Created on Fri Nov 29 15:10:51 2024

@author: hrumayor
"""

import os
import pickle
import networkx as nx
from datetime import datetime
import concurrent.futures

def extract_hour_from_date (date_string):
    
    # Parse the string into a datetime object
    date_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')

    # Extract the hour
    return date_object.hour
    
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

def has_mentions(graph, msg_id):
    """
    Determine if a message is original or created in response to another message.
    
    :param graph: The graph.
    :param msg_id: The message ID to check.
    :return: True if the message is original, False if it is a response.
    """
    # Check if the message has incoming edges with "replies" relation
    incoming_replies = [
        edge for edge in graph.in_edges(msg_id, data=True)
        if edge[2].get("relation") == "replies"
    ]
    return len(incoming_replies) == 0  # Original if no incoming replies

def message_has_mentions(msg_id):
    
    # Get only author type sucessors
    mentions = [
        neighbor for neighbor in g.neighbors(msg_id)
        if g.nodes[neighbor].get("node_type") == "author" and
           g.edges[msg_id, neighbor].get("relation") == "mention"
    ]    
        
    return mentions

def determine_message_author(msg_id):
    
    # Get only author type predecessors
    author = [
        predecessor for predecessor in g.predecessors(msg_id)
        if g.nodes[predecessor].get("node_type") == "author" and
           g.edges[predecessor, msg_id].get("relation") == "posted"
    ]
    
    return author[0]
    

def determine_message_is_created_as_reply(msg_id):
    
    # Get message type sucesor
    msg = [
        neighbor for neighbor in g.neighbors(msg_id)
        if g.nodes[neighbor].get("node_type") == "msg" and
           g.edges[msg_id, neighbor].get("relation") == "replies"
    ] 
    return msg

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
    for msg_id in message:
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


def calculate_author_influence(graph, authors):
    # Calculate betweenness centrality for the entire graph
    centrality = nx.betweenness_centrality(graph, normalized=True)

    # Filter and get the betweenness centrality for only the author nodes
    author_influence = {author: centrality[author] for author in authors if author in centrality}
    
    return author_influence

root_path = "C:/DATA_SCIENCE_HAIZEA/tmp/all-rnr-annotated-threads/charliehebdo-all-rnr-threads"
preprocess_folder = os.path.join(root_path,"preprocess")
json_folder = os.path.join(preprocess_folder, "jsons")
graph_folder = os.path.join(root_path, "graph")

with open(os.path.join(graph_folder,'graph.pkl'), 'rb') as f:
    g = pickle.load(f)    
    
    
# GRAPH ANALYSIS 
total_msg = [node for node, attrs in g.nodes(data=True) if attrs.get("node_type") == "msg"]
print(f"The graph has in total: {len(total_msg)} messages")

# Define futures to handle concurrent execution
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Start calculating retweets and favourites concurrently
    retweets_future = executor.submit(calculate_retweets, total_msg)
    favourites_future = executor.submit(calculate_favourites, total_msg)
    
    # While the retweets and favourites are being calculated, proceed with other tasks
    propagation_dict = {}
    for msg_id in total_msg:
        visited = set()
        propagation_dict[msg_id] = calculate_reverse_propagation(g, msg_id, visited)
    
    # Get the results of the retweets and favourites once they're done
    retweets_dict = retweets_future.result()
    favourites_dict = favourites_future.result()
    
    # Extract author list and calculate author influence concurrently
    total_authors = [node for node, attrs in g.nodes(data=True) if attrs.get("node_type") == "author"]
    print(f"The graph has in total: {len(total_authors)} authors.")
    
    author_influence_future = executor.submit(calculate_author_influence, g, total_authors)
    author_influence_dict = author_influence_future.result()
    print("Authors influence calculated.")



# Get information for each message
for msg_id in total_msg:
    
    print("Message_id:", msg_id , "data: ", g.nodes[msg_id])
    
    # message creation hour
    msg_hour = extract_hour_from_date(g.nodes[msg_id]["date"])
    print ("Hour:", msg_hour)
    
    # is rumour or not
    is_rumour = g.nodes[msg_id]["rumour"]
    print ("is_rumour:", is_rumour)
    
    # text information: main keys, sentiment and if has links it
    text = g.nodes[msg_id]["text"]
    print ("text:", text)
    
    # propagate to N message
    propagate_to_msg = propagation_dict[msg_id]
    print ("propagate_to_msg:", propagate_to_msg)        
    
    # who is the author?
    author = determine_message_author(msg_id)
    print ("author:", author)
    
    # is the author influyent in the graph?
    author_influence = author_influence_dict[author]
    print ("author_influence:", author_influence)
    
    # has mentions
    has_mentions = False
    mentions = message_has_mentions(msg_id)
    if len(mentions)>0:
        has_mentions = True
    print ("has_mentions:", has_mentions)
    print ("mentions:", len(mentions))
    
    # message created as reply
    is_reply_message = False
    if len(determine_message_is_created_as_reply(msg_id)) > 0:
        is_reply_message = True
    print ("is_reply_message:", is_reply_message)
        
    # message retweeted
    retweets = retweets_dict[msg_id]
    print ("retweets:", retweets)
    
    # message favourited
    favourites = favourites_dict[msg_id]
    print ("favourites:", favourites)
   
    
    input()


'''  
- noiz bidalia
- originala dan
- erantzuna dan
- retweeteatu dan
- faborito dan
- zenbat mezu sortarazi ditxun
- mentzioak dauzkan
- zein autoreena dan
- autorea influyentea ote dan
- zein hitz klabe dauzkan, zer sentimentu daukan, loturarik ote daukan
- rumorea dan ala ez
'''