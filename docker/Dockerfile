FROM cassandra:latest

# about the custom image
LABEL maintainer="datadiver.dev"
LABEL version="0.1"
LABEL description="Cassandra, Python and Jupyter Lab"

# root user
USER root:root

# install deb essential packages
RUN DEBIAN_FRONTEND=noninteractive set -eux && \
    apt-get update && \
    apt-get install -y --no-install-recommends sudo python3-pip python3-dev python3-setuptools

# create user
ENV USER=developer
ENV HOME=/home/$USER
ENV PATH $PATH:$HOME/.local/bin
RUN groupadd -g 1000 $USER && useradd -u 1000 -g $USER -G sudo -m -s /bin/bash $USER && \
    sed -i /etc/sudoers -re 's/^%sudo.*/%sudo ALL=(ALL:ALL) NOPASSWD: ALL/g' && \
    sed -i /etc/sudoers -re 's/^root.*/root ALL=(ALL:ALL) NOPASSWD: ALL/g' && \
    sed -i /etc/sudoers -re 's/^#includedir.*/## **Removed the include directive** ##"/g' && \
    echo "${USER} ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# set init script
RUN mkdir -p /opt/app/bin
COPY ./docker/init.sh ./docker/init-cassandra.sh ./docker/init-jupyter.sh /opt/app/bin/
RUN chmod +x /opt/app/bin/*.sh && \
    chown -R $USER:$USER /opt/app/bin

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

# server ports
EXPOSE 9042
EXPOSE 80

# container init script
CMD [ "/opt/app/bin/init.sh" ]
