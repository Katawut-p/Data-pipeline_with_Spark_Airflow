FROM apache/airflow:2.2.1

USER root
########################### Install Java #############################################
# Install OpenJDK-8
RUN apt-get update && \
    apt-get install \
    -y openjdk-11-jdk-headless && \
    rm -rf /var/lip/apt/list/*

# Setup JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME
########################### complete Install Java ####################################

########################### Install Spark #############################################
# install Spark 3.1.2
RUN apt-get -y install wget
WORKDIR /usr/
RUN wget https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop2.7.tgz \ 
    -O spark.tgz && \
    tar xvf spark.tgz

# Setup SPARK_HOME
ENV SPARK_HOME /usr/spark-3.1.2-bin-hadoop2.7
RUN export SPARK_HOME
########################### complete Install Spark ####################################

# Install PYTHON requirements
WORKDIR /usr/local/spark/app
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

WORKDIR /