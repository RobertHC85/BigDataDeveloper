from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.datasets import Dataset

dataset_final_1 = Dataset('dataset_dag_S5_ej2_2')
dataset_final_2 = Dataset('dataset_dag_S5_ej2_3')

# DAG que depende de la finalización de DAG2 y DAG3
with DAG(
    dag_id='final_dag_ej2',
    start_date=datetime(2025, 6, 3),
    schedule_interval=[dataset_final_1, dataset_final_2],  # Se ejecuta manualmente o cuando se configure
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    # Depender de DAG2 y DAG3
    start >> finish
