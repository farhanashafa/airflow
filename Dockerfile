FROM apache/airflow:2.10.2

USER root
# Install additional Python packages
RUN pip install pika

USER airflow
