from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime
from airflow.operators.empty import EmptyOperator

# Definir el dataset
dataset_dag1 = Dataset('dataset_dag_S5_ej2')

dataset_dag1_3 = Dataset('dataset_dag_S5_ej2_3')

# DAG que consume el dataset
with DAG(
    dag_id='consumer_dag_S5_ej2_2',
    start_date=datetime(2025, 6, 3),
    schedule=[dataset_dag1],  # Se ejecuta cuando dataset_dag1 esté disponible
    catchup=False,
) as dag:
    def analyze_data():
        print("consumer_dag_S5_ej2_2 consumió el dataset producer_dag_S5_ej2")

    consume_data = PythonOperator(
        task_id='consume_data_dag_S5_ej2_2',
        python_callable=analyze_data,
    )

    produce_data = EmptyOperator(
        task_id='produce_data_dag_S5_ej2_3',
        outlets=[dataset_dag1_3],  # Este DAG produce el dataset
    )

consume_data >> produce_data