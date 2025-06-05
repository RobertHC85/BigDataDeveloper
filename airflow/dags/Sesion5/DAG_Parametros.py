from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

def etl_process(**context):
    file_name = context['params'].get('file_name', 'file_input.csv')
    max_rows = int(context['params'].get('max_rows', 50))
    file_path = f"/opt/airflow/dags/Sesion5/data/{file_name}"

    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    df = pd.read_csv(file_path).head(max_rows)
    print(df.head())

with DAG(
    dag_id='etl_with_trigger_params',
    start_date=datetime(2025, 6, 3),
    schedule_interval=None,
    catchup=False,
    params={
        "file_name": "file_input.csv",
        "max_rows": 10
    }
) as dag:

    etl_task = PythonOperator(
        task_id='run_etl_with_params',
        python_callable=etl_process,
        provide_context=True,
    )
