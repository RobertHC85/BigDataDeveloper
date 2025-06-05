from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime
import logging

# Definir el dataset
mi_dataset = Dataset("my_dataset_s5")

# Función que simula la producción del dataset
def producir_dataset():
    logging.info("Dataset producido desde producer_dag_s5.")

# DAG que produce el dataset
with DAG(
    dag_id='producer_dag_s5',
    start_date=datetime(2025, 6, 3),
    schedule_interval='@daily',
    catchup=False,
) as dag1:
    produce = PythonOperator(
        task_id='produce_data_s5',
        python_callable=producir_dataset,
        outlets=[mi_dataset],  # Declara que esta tarea produce el dataset
    )
