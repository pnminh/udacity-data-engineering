FROM jupyter/all-spark-notebook:spark-2

WORKDIR "${HOME}"
USER root
ENV PACKAGES="libpq-dev"
COPY requirements.txt .
RUN apt-get update && apt-get install -y $PACKAGES && pip install -r requirements.txt && \
    wget https://repos.spark-packages.org/saurfang/spark-sas7bdat/3.0.0-s_2.11/spark-sas7bdat-3.0.0-s_2.11.jar -P $SPARK_HOME/jars/ && \
    wget https://repo1.maven.org/maven2/com/epam/parso/2.0.11/parso-2.0.11.jar -P $SPARK_HOME/jars/
USER jovyan