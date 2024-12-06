# FEDERATED-RUMOUR-GRAP-LEARNING

-   **Author**: Haizea Rumayor Lazkano
-   **Last update**: December 2024

------------------------------------------------------------------------

This GitHub project presents a prototype that leverages `federated learning` to learn message rumour patterns in social networks based on their characterization and message propagation.

## Overview

The project uses the `PHEME dataset of rumours and non-rumours` for the analysis. This dataset contains a collection of Twitter rumours and non-rumours posted during breaking news events and is accessible at: [PHEME dataset of rumours and non-rumours](https://figshare.com/articles/dataset/PHEME_dataset_of_rumours_and_non-rumours/4010619?file=6453753).

In the `federated learning` framework, the project leverages a server and multiple client nodes. Each client node analyzes a specific topic from the `PHEME dataset` by constructing graphs, summarizing the information, and sending it to the server for further analysis.

The server receives the messages, groups them, preprocesses the data, and trains a `Random Forest model` to detect patterns that determine whether a message is a rumour.  The model can identify key features such as propagation, author, mentions, emotions, and other related factors. Although the results are not returned to the client in this pilot phase, in future iterations, they could be used to classify messages as rumours or not, based on the patterns learned.

## Key Features

- **Federated Learning**: The system operates in a federated learning setup, where client nodes analyze data locally and send summarized information to a central server for further processing and model training.
  
- **Graph-Based Analysis**: Each client builds message-related graphs based on the PHEME dataset, analyzing the relationships between authors, messages, and message propagation.

- **Random Forest Classifier**: The server trains a Random Forest model to detect patterns in the propagation of rumours and identify key features that distinguish rumours from non-rumours.

- **MQTT Communication**: MQTT (Message Queuing Telemetry Transport) is used for lightweight and efficient communication between the client nodes and the server. This protocol allows for quick data transmission, ensuring minimal latency while sending the summarized information from the client to the server.

- **Scalable Architecture**: The federated learning setup allows the system to scale across multiple clients and datasets, improving efficiency and privacy.
  
- **Future Expansion**: While the pilot phase focuses on training the model, future iterations could enable real-time classification of messages as rumours or not, based on the learned patterns.


## Project Structure

The project is organized into the following directories and files:

- **modules/**: Contains modules with functions used throughout the project.
- **src/**: Contains Python scripts that implement the core functionality. Three scripts are located here:
  - `dispatcher.py`
  - `client.py`
  - `server.py`
- **requirements.txt**: Lists the Python packages required to run the project.
- **Dockerfile**: Used to launch the project in a Docker container.
- **config.yaml**: Contains configuration settings for MQTT communication between the client nodes and the server.
- **data_config.yaml**: Contains information about hte datasets to be downloaded and analyzed by each client.

## Yaml File Content Overview

### **config.yaml**

This file contains configuration settings for MQTT communication between the client nodes and the server. It includes:
- **MQTT_BROKER**: The broker address.
- **MQTT_PORT**: The MQTT port.
- **MQTT_TOPIC**: The topic to subscribe to and listen for messages.
- **MQTT_KEEPALIVE**: The keep-alive duration of the MQTT client, which helps maintain the connection.
- **TIME_LISTENING_MESSAGES**: The maximum listening time for receiving messages.
- **VISUALIZE_TREE**: Indicates whether the tree should be displayed or not.

### **data_config.yaml**

This file contains information about the datasets to be downloaded and analyzed. It includes:
- **DOWNLOAD_DATA_URL**: The URL for downloading the `PHEME dataset`.
- **DATA_PATH**: The local directory path where the dataset should be stored on the client side.
- **THEMES**: A mapping of themes to dataset names, helping the client determine which datasets to analyze.

## Installation and Run Steps

To ensure a clean and isolated environment for this project, a `Dockerfile` is provided to launch Docker locally and run Python 3.9 containers. This approach ensures that the local environment remains unaffected and that all dependencies are installed within the container.

---

### Prerequisites:

- **Docker** must be installed on your machine. Download and install Docker from the [official website](https://www.docker.com/get-started).

---

### Building and Running the Client Docker Image

1. **Navigate to the Project Directory**  
   Open your terminal and navigate to the root directory of the project where the `Dockerfile` is located.

2. **Build the Client Docker Image**  
   Build a Docker image for the client using the following command:
   ```bash
   docker build -t client1 --build-arg ROLE=CLIENT_1 .
   ```
   The ROLE=CLIENT_1 argument defines the role of the Docker image as a client. The identifier CLIENT_1 distinguishes this client instance from others. For additional client nodes, modify the identifier (e.g., CLIENT_2, CLIENT_3).

   Example for multiple client nodes:
   ```bash
   docker build -t <client_app_identification> --build-arg ROLE=CLIENT_<identification> .
   ```
  The role is saved into a `role.txt` file in the container and used by the `dispatcher.py` script to determine execution behavior.


3. **Run the Client Docker Image**
    Launch the client container with this command:
    ```bash
    docker run -it --rm -v ${PWD}:/app <client_app_identification>
    ```
    This will launch the client container and mount the current directory `${PWD}` into the `/app` directory inside the container. This allows the client to access the necessary files from the host system.

### Building and Running the Server Docker Image

### Visualizing Trees and Rules (VISUALIZE_TREE = 1)

1. **Configure Matplotlib for Docker GUI Support: Install XLaunch**  
   - Download and install **XLaunch** from the [Xming website](https://sourceforge.net/projects/xming/).  
   - Configure XLaunch as follows:  
     - Choose **"Multiple windows"**.  
     - Set the display number to **0**.  
     - Select **"Start No client"**.  
     - Choose **"Native OpenGL"**.  
     - Check **"No access control"**.

2. **Get Your Windows IP Address**  
   Run this command in Command Prompt to find your IP address:
   ```bash
   ipconfig
   ```
   Note the IPv4 Address (e.g., `192.168.1.100`).

3. **Navigate to the Project Directory**:
   Open your terminal and navigate to the root directory of the project.

4. **Build the Server Docker Image**:
   Use this command to build a server Docker image:
   ```bash
   docker build -t server --build-arg ROLE=SERVER .
   ```
   You must only create one.
   ```bash
   docker build -t <server_app_identification> --build-arg ROLE=SERVER .
   ```

5. **Run the Server Docker Image**:
    Run the server container with GUI support:
    ```bash
    docker run --rm -it --env=DISPLAY=<ip_address>:0 -v="$(Get-Location):/app" <server_app_identification>
    ```
    Replace <ip_address> with your Windows IP address and <server_app_identification> with the actual server image tag.
    This mounts the current directory `${PWD}` to `/app` inside the container, allowing access to necessary files and allows also to display the tree in apart panel.

### Without Tree and Rules Visualization (VISUALIZE_TREE = 0)

1. **Navigate to the Project Directory**:
   Open your terminal and navigate to the root directory of the project.

2. **Build the Server Docker Image**:
   Build a server Docker image using:
   ```bash
   docker build -t server --build-arg ROLE=SERVER .
   ```

2. **Run the Server Docker Image**:
    Launch the server container without GUI support:
    ```bash
    docker run -it --rm -v ${PWD}:/app <server_app_identification>
    ```
    This mounts the current directory `${PWD}` to `/app` inside the container, allowing access to necessary files.


## Dispatcher Script Overview

The `dispatcher.py` script is responsible for determining the role of the application (either **SERVER** or **CLIENT**) and dispatching the corresponding tasks accordingly. 

- **Role Determination**:  
  The script reads the role from a file `role.txt` located in the `tmp` directory created during image build process. Based on the role read from the file, the script determines whether to start the **server** or **client** process. 

- **Task Dispatch**:  
  If the role is identified as **SERVER**, the script executes the `server.py` script. Otherwise, it runs the `client.py` script. This ensures that the correct functionality (client or server) is triggered based on the role assigned.

The `dispatcher.py` helps manage the orchestration of different components in the system, ensuring that the correct process is initiated for each node.

## Client Script Overview

The `client.py` script is responsible for downloading, preparing, analyzing, and sending data to the server in a federated learning framework. It performs the following key tasks:

- **Role Determination**:  
  The script starts by reading the role of the node from a `role.txt` file located in the `tmp` directory. It extracts the client identifier (ID) and uses it to select a specific dataset (theme) for analysis.

- **Data Preparation**:  
  Based on the selected theme, the client downloads and preprocesses the corresponding data from the PHEME dataset. The data is cleaned, structured, and stored in specific folders for further analysis. This involves organizing the data into folders for preprocessing, graphs, and JSON files.

- **Graph Creation**:  
  The script generates a message relation graph from the preprocessed data. This graph represents the relationships between messages in the dataset, which will be analyzed to detect patterns in the message propagation.

- **Graph Analysis and Data Extraction**:  
  After creating the graph, the client analyzes it to extract relevant features related to message propagation. The extracted information is saved in a file, which is then split into smaller batches for easier transmission. 

  The key features extracted from the messages are:
  - **Message Propagation**: The spread of messages throughout the network, analyzing how they are passed from one user to another.
  - **Mentions**: Identifies whether the message includes other users or topic mentions, helping to understand its connections and context.
  - **Hashtags and Links Relation**: Detects whether the message contains hashtags or links, and how these elements contribute to its spread.
  - **Emotions**: Analyzes the emotional tone of the message, such as whether the message expresses positivity, negativity, or neutrality.
  - **Retweets and Favorites**: Tracks whether the message has been retweeted or favorited, which can indicate its reach and popularity within the network.
  - **Other Relevant Features**: Any additional data points that can help in understanding the message's impact and relevance in the social network.

  **Note**: Although attempts were made to capture the **author's influence** in the network using the **betweenness centrality** measure, this approach has been excluded in the initial pilot due to the large size of the graphs. Future versions may revisit this analysis to provide deeper insights into the role of individual users in message diffusion.

- **Data Splitting and Sending**:  
  The script splits the prepared data into smaller zipped files and stores them in a designated folder. These files are then sent to the server via MQTT, where they can be further processed and analyzed.

  The client uses the MQTT protocol to send the data to the server. It connects to the broker specified in the `config.yaml` file and transmits the data in batches, allowing for efficient communication in a federated learning environment.

This script allows the client node to participate in a federated learning system, where it processes and analyzes data locally before sharing the results with the central server.


## Server Script Overview

The `server.py` script is responsible for coordinating the federated learning process by receiving data from multiple client nodes, aggregating and preprocessing the received data, and training a machine learning model to detect rumour patterns in the messages.

- **Message Reception for a While**:  
The server connects to the MQTT broker and subscribes to the specified topic. It listens for incoming messages from the clients for a predefined amount of time, specified in the `TIME_LISTENING_MESSAGES` parameter from the configuration file.

- **Data Aggregation and Preprocessing**:  
After receiving the messages, the server aggregates all the data into a single structure. This data is then preprocessed to ensure that it is ready for training the machine learning model. This step includes cleaning the data and organizing it into a suitable format for analysis.

- **Model Training**:  
With the preprocessed data, the server proceeds to train a **Random Forest model**. The model learns from the patterns in the data, helping to identify characteristics that distinguish rumour messages from non-rumours.

- **Pattern and Rules Extraction**:  
After the model is trained, it is used to extract rules or patterns that define which features are indicative of rumour messages. These patterns include factors such as message propagation, mentions, hashtags, emotional tone, and other relevant features that help in determining whether a message is a rumour.

