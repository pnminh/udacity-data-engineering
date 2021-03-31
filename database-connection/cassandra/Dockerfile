FROM jupyter/minimal-notebook:latest
USER root
ENV PACKAGES="libpq-dev" \
    PIP_PACKAGES="cassandra-driver"
#RUN apt-get update && apt-get install -y $PACKAGES && pip install $PIP_PACKAGES
RUN apt-get update && pip install $PIP_PACKAGES
USER jovyan