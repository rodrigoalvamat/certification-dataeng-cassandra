#!/bin/bash

# init jupyter server
sudo -b -u "$USER" -s /opt/app/bin/init-jupyter.sh

# start postgres server
sudo -u cassandra -s /opt/app/bin/init-cassandra.sh
