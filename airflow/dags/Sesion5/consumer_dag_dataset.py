from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime
import logging

# Definir el dataset
mi_dataset = Dataset("my_dataset_s5")

# Función que se ejecuta cuando se consume el dataset
def consumir_dataset():
    logging.info("Dataset consumido correctamente desde consumer_dag_s5.")

# DAG que consume el dataset
with DAG(
    dag_id='consumer_dag_s5',
    start_date=datetime(2025, 6, 3),
    schedule=[mi_dataset],
    catchup=False,
) as dag2:
    consume = PythonOperator(
        task_id='consume_data_s5',
        python_callable=consumir_dataset,
    )
