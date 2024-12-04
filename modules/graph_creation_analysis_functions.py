# GRAPH RELATED FUNCTIONS
import os
import json
import networkx as nx
import pickle
from datetime import datetime
import re
from textblob import TextBlob
import pandas as pd

def add_to_graph (graph, msg_id , msg, verbose):
    """
    Function that adds message, author, and mentions as nodes and creates relations in the graph.

    Parameters:
        graph (networkx.DiGraph): The directed graph to add nodes and edges to.
        msg_id (str): The unique ID of the message.
        msg (dict): A dictionary containing message data (text, author, mentions, replies).
        verbose (bool): If True, prints information about the nodes and edges being added.

    Returns:
        Add graph nodes and relations.
    """
    
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
    """
    Function that creates a directed graph from JSON files and saves it.

    Parameters:
        json_folder (str): Path to the folder containing JSON files.
        graph_folder (str): Path to the folder where the graph will be saved.
        verbose (bool): If True, prints the file being processed.

    Returns:
        None: Creates and save the graph to be analyze.
    """

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
def extract_hour_from_date(date_string):
    """
    Function that extracts the hour from a date_string.

    Parameters:
        date_string (str): Date string that corresponds to message creation.
       
    Returns:
        int: Returns the hour of message creation.
    """
    date_object = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
    return date_object.hour

def calculate_reverse_propagation(graph, msg_id, visited, propagation_dict):
    """
    Function that calculates the reverse propagation count for a message in a graph.

    Parameters:
        graph (networkx.DiGraph): The directed graph containing messages and relationships.
        msg_id (str): The unique ID of the message to calculate propagation for.
        visited (set): A set of already visited message IDs to avoid cycles.
        propagation_dict (dict): A dictionary tracking the propagation count for each message.

    Returns:
        int: The propagation count for the given message.
    """
    if msg_id in visited:
        return 0
    visited.add(msg_id)
    # Obtain all predecessors of the message
    msg_predecessors = [pred for pred in graph.predecessors(msg_id) if graph.nodes[pred].get("node_type") == "msg"]
    propagation_count = sum(propagation_dict.get(pred, 0) + 1 for pred in msg_predecessors)
    propagation_dict[msg_id] = len(msg_predecessors) + propagation_count
    return propagation_dict[msg_id]

def message_has_mentions(msg_id, g, mentions_cache):
    """
    Function that checks if a message has mentions and caches the result.

    Parameters:
        msg_id (str): The unique ID of the message.
        g (networkx.DiGraph): The directed graph containing messages and relationships.
        mentions_cache (dict): A cache dictionary to store mentions for each message ID.

    Returns:
        list: A list of authors mentioned in the given message.
    """
    if msg_id in mentions_cache:
        return mentions_cache[msg_id]
    mentions = [
        neighbor for neighbor in g.neighbors(msg_id)
        if g.nodes[neighbor].get("node_type") == "author" and g.edges[msg_id, neighbor].get("relation") == "mention"
    ]
    mentions_cache[msg_id] = mentions
    return mentions

def determine_message_author(msg_id, g, author_cache):
    """
    Function that determines the author of a given message and caches the result.

    Parameters:
        msg_id (str): The unique ID of the message.
        g (networkx.DiGraph): The directed graph containing messages and relationships.
        author_cache (dict): A cache dictionary to store authors for each message ID.

    Returns:
        str or None: The author of the message, or None if no author is found.
    """
    if msg_id in author_cache:
        return author_cache[msg_id]
    author = [
        predecessor for predecessor in g.predecessors(msg_id)
        if g.nodes[predecessor].get("node_type") == "author" and g.edges[predecessor, msg_id].get("relation") == "posted"
    ]
    author_cache[msg_id] = author[0] if author else None
    return author_cache[msg_id]

def calculate_retweets(message, g, retweet_cache):
    """
    Function that calculates the total retweets for a set of messages and caches the results.

    Parameters:
        message (list): A list of message IDs to calculate retweets for.
        g (networkx.DiGraph): The directed graph containing messages and relationships.
        retweet_cache (dict): A cache dictionary to store retweet counts for each message ID.

    Returns:
        dict: A dictionary with message IDs as keys and total retweets as values.
    """
    retweet_data = {}
    for msg_id in message:
        if msg_id in retweet_cache:
            retweet_data[msg_id] = retweet_cache[msg_id]
        else:
            reply_edges = [
                edge for edge in g.in_edges(msg_id, data=True)
                if edge[2].get("relation") == "replies"
            ]
            total_retweets = sum(edge[2].get("retweet", 0) for edge in reply_edges)
            retweet_cache[msg_id] = total_retweets
            retweet_data[msg_id] = total_retweets
    return retweet_data

def calculate_favourites(message, g, favourite_cache):
    """
    Function that calculates the total favourite counts for a set of messages and caches the results.

    Parameters:
        message (list): A list of message IDs to calculate retweets for.
        g (networkx.DiGraph): The directed graph containing messages and relationships.
        favourite_cache (dict): A cache dictionary to store favourite counts for each message ID.

    Returns:
        dict: A dictionary with message IDs as keys and total favourite counts as values.
    """
    favourite_data = {}
    for msg_id in message:
        if msg_id in favourite_cache:
            favourite_data[msg_id] = favourite_cache[msg_id]
        else:
            reply_edges = [
                edge for edge in g.in_edges(msg_id, data=True)
                if edge[2].get("relation") == "replies"
            ]
            total_favourites = sum(edge[2].get("favourite", 0) for edge in reply_edges)
            favourite_cache[msg_id] = total_favourites
            favourite_data[msg_id] = total_favourites
    return favourite_data

def detect_emotion(text):
    """
    Function that detects emotion of the text.

    Parameters:
        text (str): Message text

    Returns:
        str: returns Positive, Negative or Neutral emotion of the text
    """
    blob = TextBlob(text)
    polarity = blob.sentiment.polarity
    return "Positive" if polarity > 0 else "Negative" if polarity < 0 else "Neutral"

def analyze_and_tokenize_text(text):
    """
    Function that processes and tokenizes a text, removing links, hashtags, punctuation, numbers, and stopwords.

    Parameters:
    text (str): The input text to be analyzed and tokenized.

    Returns:
    tuple: A tuple containing:
        - A list of tokens (words) after processing.
        - A boolean indicating if the text contains links.
        - A boolean indicating if the text contains hashtags.
        - A list of hashtags found in the text.
    """
    # Lower the text
    new_text = text.lower()

    # Find links and remove 
    link_regex = re.compile(r'http\S+')
    has_link =  bool(link_regex.search(new_text))
    new_text = re.sub(link_regex, ' ', new_text).strip()

    # Find hashtags, take them and remove
    hashtag_regex = re.compile(r'#\w+')
    has_hashtags = bool(hashtag_regex.search(new_text))
    hashtags = hashtag_regex.findall(new_text)
    new_text = re.sub(hashtag_regex, ' ', new_text).strip()   

    # Find and remove punctuation marks
    punctuation_regex = re.compile(r'[!,\-\/\\.;:?=<>\[\]]')
    new_text = re.sub(punctuation_regex , ' ', new_text)

    # Remove numbers from text
    new_text = re.sub("\d+", ' ', new_text).strip()

    # Remove multiple spaces from text
    new_text = re.sub("\\s+", ' ', new_text)

    # Find and remove stop words like prepositions, 
    stop_words = {'also', 'at', 'to', 'for', 'in', 'on', 'by', 'with', 'as', 'from', 'of', 'about',"a", "an", "that",
                  'the', 'but', 'is','were', 'you', 'me', 'they', 'which', "we're", "that's", "are", "or", "do", "did",
                  "isn't",'am', "I", 'us'}
    new_text = [token for token in new_text.split() if token not in stop_words and len(token) > 1]

    # Return tokens, whether has links, whether has hashtags, and the hashtags in it
    return new_text, has_link, has_hashtags, hashtags


def extract_msg_information(df, graph, msg_id, retweets_dict, favourites_dict, propagation_dict):
    """
    Function that extracts information about a message from the graph and updates the provided DataFrame.

    Parameters:
        df (pandas.DataFrame): The DataFrame to update with the new message information.
        graph (networkx.DiGraph): The directed graph containing message data and relationships.
        msg_id (str): The unique ID of the message to extract information for.
        retweets_dict (dict): A dictionary containing retweet counts for messages.
        favourites_dict (dict): A dictionary containing favourite counts for messages.
        propagation_dict (dict): A dictionary tracking the propagation count for each message.

    Returns:
        pandas.DataFrame: The updated DataFrame with the new message information.
    """    
    msg_hour = extract_hour_from_date(graph.nodes[msg_id]["date"])
    is_rumour = graph.nodes[msg_id]["rumour"]
    author = determine_message_author(msg_id, graph, {})  # Corrected call with required arguments
    mentions = message_has_mentions(msg_id, graph, {})  # Corrected call with required arguments
    is_reply_message = bool(graph.neighbors(msg_id))
    retweets = retweets_dict[msg_id]
    favourites = favourites_dict[msg_id]
    text = graph.nodes[msg_id]["text"]
    emotion = detect_emotion(text)
    tokens, has_link, has_hashtag, hashtags = analyze_and_tokenize_text(text)

    # Calculate propagation
    visited = set()
    propagate_to_msg = calculate_reverse_propagation(graph, msg_id, visited, propagation_dict)

    # Create a new row dictionary
    new_row = {
        "msg_id": msg_id, "msg_hour": msg_hour, "propagate_to_msg": propagate_to_msg,
        "has_mentions": len(mentions) > 0, "mentions": len(mentions),
        "is_reply_message": is_reply_message, "retweets": retweets, "favourites": favourites,
        "text": text, "tokens": tokens, "has_link": has_link,
        "has_hashtag": has_hashtag, "hashtags": hashtags, "emotion": emotion,
        "author": author, "is_rumour": is_rumour
    }

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    return df

def get_msg_information(graph, save_path):
    """
    Extracts message information from the graph and returns a DataFrame.
    """
    propagation_dict = {}
    visited = set()
    total_msg = [node for node, attrs in graph.nodes(data=True) if attrs.get("node_type") == "msg"]
    
    # Initialize an empty DataFrame with defined columns
    columns = [ "msg_id", "msg_hour", "propagate_to_msg",
        "has_mentions", "mentions", "is_reply_message", "retweets" , "favourites" ,
        "text" , "tokens" , "has_link" ,"has_hashtag" , "hashtags", "emotion",
        "author", "is_rumour"]
    df = pd.DataFrame(columns=columns)
    
    retweets_dict = {}
    favourites_dict = {}

    for msg_id in total_msg:
        # Calculate retweets and favourites for each message
        retweets_dict = calculate_retweets([msg_id], graph, retweets_dict)
        favourites_dict = calculate_favourites([msg_id], graph, favourites_dict)
        
        # Calculate propagation
        calculate_reverse_propagation(graph, msg_id, visited, propagation_dict)
        
        # Extract message information
        df = extract_msg_information(df, graph, msg_id, retweets_dict, favourites_dict, propagation_dict)

    filename = "msg_summary.parquet"
    df.to_parquet(os.path.join(save_path, filename), engine ="pyarrow")
    print(f"\tMsg summary saved in : {os.path.join(save_path, filename)}")
    return os.path.join(save_path, filename)

