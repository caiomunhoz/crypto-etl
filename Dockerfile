FROM apache/airflow:3.1.3-python3.12

USER airflow

COPY dags/ /opt/airflow/dags/