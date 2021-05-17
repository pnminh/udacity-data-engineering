FROM jupyter/pyspark-notebook:latest
USER root
ENV PACKAGES="libpq-dev" \
    PIP_PACKAGES="pyspark"
RUN apt-get update && apt-get install -y $PACKAGES && pip install $PIP_PACKAGES && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar -P $SPARK_HOME/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar -P $SPARK_HOME/jars/
USER jovyan