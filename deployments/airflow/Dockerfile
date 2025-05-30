FROM apache/airflow:2.10.5-python3.11

USER root

# Install apt packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Download spark drivers
RUN mkdir -p ${AIRFLOW_HOME}/jars
RUN curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar -o ${AIRFLOW_HOME}/jars/gcs-connector-hadoop3-latest.jar

# Setup PYTHON_PATH
ENV PYTHONPATH=${PYTHONPATH}:/opt/airflow/dags/repo

# Install python dependencies
RUN pip install --no-cache-dir \
    "apache-airflow[google]==${AIRFLOW_VERSION}" \
    apache-airflow-providers-apache-spark==5.0.0 \
    acryl-datahub==1.0.0.2 \
    soda-core-bigquery==3.5.2
