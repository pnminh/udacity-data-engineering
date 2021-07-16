FROM jupyter/pyspark-notebook:latest
USER root
ARG spark_version="2.4.7"
ENV PACKAGES="libpq-dev"
COPY requirements.txt .
RUN apt-get update && apt-get install -y $PACKAGES && pip install -r requirements.txt && \
    wget https://repos.spark-packages.org/saurfang/spark-sas7bdat/3.0.0-s_2.11/spark-sas7bdat-3.0.0-s_2.11.jar -P /home/jovyan
USER jovyan