#!/bin/bash

# docker image name
IMAGE="datadiverdev/cassandra-jupyter"

# docker application hostname
HOST="localhost"

# docker application name
NAME="datadiver-cassandra"

# docker application ports
PORT_CASSANDRA="9042:9042"
PORT_JUPYTER="80:80"

# run the docker image
cmd=(docker run --name "$NAME" --hostname "$HOST" -p "$PORT_CASSANDRA" -p "$PORT_JUPYTER" -d "$IMAGE")
"${cmd[@]}"
