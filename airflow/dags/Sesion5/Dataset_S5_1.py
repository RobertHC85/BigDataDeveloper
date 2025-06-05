from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import datetime

# Definir el dataset
dataset_dag1 = Dataset('dataset_dag_S5_ej2')

# DAG que produce el dataset
with DAG(
    dag_id='producer_dag_S5_ej2',
    start_date=datetime(2025, 6, 3),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='inicio')
    produce = EmptyOperator(
        task_id='produce_data_ej2',
        outlets=[dataset_dag1],  # Este DAG produce el dataset
    )
    end = EmptyOperator(task_id='fin')

    start >> produce >> end
