FROM jupyter/all-spark-notebook:python-3.11

USER root

RUN curl -O https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz \
    && tar zxvf spark-3.5.1-bin-hadoop3.tgz \
    && rm -rf spark-3.5.1-bin-hadoop3.tgz \
    && mv spark-3.5.1-bin-hadoop3/ /usr/local/ \
    && rm -rf /usr/local/spark \
    && rm -rf /usr/local/spark-3.4.3-bin-hadoop3 \
    && ln -s /usr/local/spark-3.5.1-bin-hadoop3 /usr/local/spark

RUN pip install --default-timeout=100 pyspark==3.5.1