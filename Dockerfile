# syntax=docker/dockerfile:1

FROM python:3.11-bullseye
RUN apt-get update
RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y python3-pip
RUN pip3 install --upgrade pip

ENV USER=ubuntu
RUN adduser -u 1026 $USER
RUN usermod -a --group users $USER     # Adding user to group $USER

#Enriching PATH
ENV HOME=/home/$USER
ENV PATH=$PATH:$HOME/.local/bin

ENV ACTUALYIELD_PATH=$HOME/actualyield
ENV DB_PATH=$HOME/actualyield/data

ENV PYTHONPATH=$ACTUALYIELD_PATH:$HOME

RUN mkdir -p $ACTUALYIELD_PATH
RUN mkdir -p $DB_PATH

RUN chown -R $USER $ACTUALYIELD_PATH
RUN chown -R $USER $DB_PATH

WORKDIR $ACTUALYIELD_PATH

# Copy pip requirements before copying full project
USER $USER
COPY --chown=$USER:$USER /requirements.txt $ACTUALYIELD_PATH

# Install Pip dependencies --> Need to install as root
RUN pip3 install -Ur $ACTUALYIELD_PATH/requirements.txt

# Now copy full project
COPY --chown=$USER:$USER / $ACTUALYIELD_PATH

RUN chmod +x $ACTUALYIELD_PATH/run.sh

# Run container as root to be able to create and write in folders
USER root
ENTRYPOINT [ "./run.sh" ]
