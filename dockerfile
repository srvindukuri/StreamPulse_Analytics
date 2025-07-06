FROM apache/airflow:2.9.2

USER root

# Install OpenJDK and Spark
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless wget curl && \
    mkdir -p /opt/spark && \
    wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.1-bin-hadoop3.tgz -C /opt/spark --strip-components=1 && \
    rm spark-3.5.1-bin-hadoop3.tgz && \
    apt-get clean

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Switch to airflow user before installing pip packages
USER airflow

# Install PySpark as airflow user
RUN pip install pyspark==3.5.1
