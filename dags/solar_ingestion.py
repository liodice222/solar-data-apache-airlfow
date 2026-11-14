from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    #ui display name
    dag_id="solar_ingestion_dag",
    description="Ingest solar data from CSV files",
    #earliest date dag scheduled to run
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["solar", "ingestion"],
) as dag:

#first task, ingest solar data from local files 
    t1 = BashOperator(
        task_id="ingest_solar_data",
        bash_command="python3 /opt/airflow/dags/solar_ingestion.py",
    )