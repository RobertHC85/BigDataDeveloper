from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Definir el DAG
with DAG(
    dag_id='run_jar_dag',
    start_date=datetime(2025, 6, 3),
    schedule_interval='@daily',  # Ejecutar diariamente
    catchup=False,
) as dag:
    
    # Definir la tarea para ejecutar el JAR
    run_jar_task = BashOperator(
        task_id='run_jar',
        bash_command='java -jar /opt/airflow/dags/Sesion5/data/HolaMundoDatapath.jar',
    )

# El DAG se ejecutará de acuerdo con el cronograma
