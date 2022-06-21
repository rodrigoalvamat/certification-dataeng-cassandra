FROM cassandra:latest

# about the custom image
LABEL maintainer="datadiver.dev"
LABEL version="0.1"
LABEL description="Cassandra, Python and Jupyter Lab"

# root user
USER root:root

# server ports
EXPOSE 7000 7001 7199 9042 9160
EXPOSE 80

# install deb essential packages
RUN DEBIAN_FRONTEND=noninteractive set -eux && \
    apt-get update && \
    apt-get install -y --no-install-recommends sudo python3-pip python3-dev python3-setuptools

# create user
ENV USER=developer
ENV HOME=/home/$USER
ENV PATH=$PATH:$HOME/.local/bin
RUN groupadd -g 1000 $USER && useradd -u 1000 -g $USER -G sudo -m -s /bin/bash $USER && \
    sed -i /etc/sudoers -re 's/^%sudo.*/%sudo ALL=(ALL:ALL) NOPASSWD: ALL/g' && \
    sed -i /etc/sudoers -re 's/^root.*/root ALL=(ALL:ALL) NOPASSWD: ALL/g' && \
    sed -i /etc/sudoers -re 's/^#includedir.*/## **Removed the include directive** ##"/g' && \
    echo "${USER} ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# set init script
RUN mkdir -p /opt/app/bin
COPY ./docker/init.sh ./docker/init-cassandra.sh ./docker/init-jupyter.sh /opt/app/bin/
RUN chmod +x /opt/app/bin/*.sh && \
    chown -R $USER:$USER /opt/app/bin && \
    sed -i /etc/cassandra/cassandra.yaml -re 's/listen_address: localhost/listen_address: 127.0.0.1/g' && \
    sed -i /etc/cassandra/cassandra.yaml -re 's/# broadcast_address: 1.2.3.4/broadcast_address: 127.0.0.1/g' && \
    sed -i /etc/cassandra/cassandra.yaml -re 's/rpc_address: localhost/rpc_address: 0.0.0.0/g' && \
    sed -i /etc/cassandra/cassandra.yaml -re 's/# broadcast_rpc_address: 1.2.3.4/broadcast_rpc_address: 127.0.0.1/g'

# set user environment
USER $USER
WORKDIR $HOME
SHELL ["/bin/bash", "--login", "-c"]
RUN echo "alias python='python3'" >> $HOME/.bash_aliases

# install python libraries
COPY ./docker/requirements.txt .
RUN pip3 install -r requirements.txt

# create project workspace
RUN mkdir -p $HOME/.jupyter $HOME/workspace
COPY ./docker/jupyter_notebook_config.py $HOME/.jupyter/jupyter_notebook_config.py
WORKDIR $HOME/workspace

# copy project files
RUN mkdir -p ./data/ ./docs/ ./images/ ./lib
COPY ./data/ ./data/
COPY ./docs/ ./docs/
COPY ./images/ ./images/
COPY ./lib/ ./lib/
COPY ./etl.ipynb ./
COPY ./etl.py ./
COPY ./LICENSE ./
COPY ./README.md ./
RUN sudo chown -R $USER:$USER $HOME

# container init script
CMD [ "/opt/app/bin/init.sh" ]
