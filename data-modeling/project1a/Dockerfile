FROM jupyter/scipy-notebook:latest
USER root
ENV PACKAGES="libpq-dev" \
    PIP_PACKAGES="psycopg2 ipython-sql"
RUN apt-get update && apt-get install -y $PACKAGES && pip install $PIP_PACKAGES
USER jovyan