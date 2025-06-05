from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime
from airflow.operators.empty import EmptyOperator

# Definir el dataset
dataset_dag1 = Dataset('dataset_dag_S5_ej2')

dataset_dag1_2 = Dataset('dataset_dag_S5_ej2_2')

# DAG que consume el dataset
with DAG(
    dag_id='consumer_dag_S5_ej2_1',
    start_date=datetime(2025, 6, 3),
    schedule=[dataset_dag1],  # Se ejecuta cuando dataset_dag1 esté disponible
    catchup=False,
) as dag:
    def process_data():
        print("consumer_dag_S5_ej2_1 consumió el dataset producer_dag_S5_ej2")

    consume_data = PythonOperator(
        task_id='consume_data_dag_S5_ej2_1',
        python_callable=process_data,
    )

    produce_data = EmptyOperator(
        task_id='produce_data_dag_S5_ej2_1',
        outlets=[dataset_dag1_2],  # Este DAG produce el dataset
    )

consume_data >> produce_data