FROM apache/airflow:3.0.6-python3.12

USER airflow

COPY dags/ /opt/airflow/dags/