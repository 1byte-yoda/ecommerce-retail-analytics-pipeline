FROM apache/airflow:slim-latest-python3.11
USER root

RUN echo "deb http://deb.debian.org/debian/ bullseye main" > /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y openjdk-11-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# For ps command not found
RUN apt-get update && apt-get install -y procps && rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

COPY requirements.txt .
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark apache-airflow-providers-celery
RUN pip install --no-cache-dir apache-airflow-providers-redis
RUN pip install --no-cache-dir -r requirements.txt
