FROM jupyter/all-spark-notebook:python-3.9

USER root

RUN curl -O https://dlcdn.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz \
    && tar zxvf spark-3.4.1-bin-hadoop3.tgz \
    && rm -rf spark-3.4.1-bin-hadoop3.tgz \
    && mv spark-3.4.1-bin-hadoop3/ /usr/local/ \
    && rm -rf /usr/local/spark \
    && rm -rf /usr/local/spark-3.4.0-bin-hadoop3 \
    && ln -s /usr/local/spark-3.4.1-bin-hadoop3 /usr/local/spark

RUN pip install pyspark==3.4.1