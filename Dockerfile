FROM apache/airflow:3.1.2-python3.13

USER airflow

COPY dags/ /opt/airflow/dags/