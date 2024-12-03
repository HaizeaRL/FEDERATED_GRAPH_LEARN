# FEDERATED_GRAPH_LEARN

# Comando to build
docker build -t server --build-arg ROLE=SERVER .
or as much is wanted
docker build -t client1 --build-arg ROLE=CLIENT_1 .
docker build -t client2 --build-arg ROLE=CLIENT_2 .

# Comando to run as bash
docker run -it --rm -v ${PWD}:/app servidor /bin/bash

# Comando to run por su cuenta
docker run -it --rm -v ${PWD}:/app servidor

# justification
the server has trained a federated learning model to detect patterns in the diffusion of rumors in messages. The model can identify key features such as propagation, author, mentions, emotions, and other related factors. Although the results are not returned to the client in this pilot phase, in future iterations, they could be used to classify messages as rumors or not, based on the patterns learned.