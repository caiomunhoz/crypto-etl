FROM apache/airflow:latest

USER airflow

COPY dags/ /opt/airflow/dags/