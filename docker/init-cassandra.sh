#!/bin/bash

# set cassandra environment 
export JAVA_HOME="/opt/java/openjdk"
export CASSANDRA_HOME="/opt/cassandra"
export PATH="$PATH:$JAVA_HOME/bin:$CASSANDRA_HOME/bin"

# start cassandra server
cassandra -f
