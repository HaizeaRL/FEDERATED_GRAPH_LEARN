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