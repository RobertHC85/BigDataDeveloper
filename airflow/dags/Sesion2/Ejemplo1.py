#DAG Simple con PythonOperator
#-----------------------------
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_world():
    print("Hello World Datapath DEP26 - Módulo 9")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 5),
    #prueba de cambio CI/CD
    #prueba de cambio CI/CD
    #prueba de cambio CI/CD        
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval=timedelta(days=1),
    #schedule_interval='0 0 * * MON-FRI',  # A las 00:00 horas de lunes a viernes
)

t1 = PythonOperator(
    task_id='hello_world_task',
    python_callable=hello_world,
    dag=dag,
)

t1
