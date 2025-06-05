#DAG con Dependencias
#-------------------
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def task1():
    print("Task 1")

def task2():
    print("Task 2")

def task3():
    print("Task 3")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dependency_dag',
    default_args=default_args,
    description='A DAG with dependencies',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='task1',
    python_callable=task1,
    dag=dag,
)

t2 = PythonOperator(
    task_id='task2',
    python_callable=task2,
    dag=dag,
)

t3 = PythonOperator(
    task_id='task3',
    python_callable=task3,
    dag=dag,
)

#t1 >> [t2, t3]
[t1, t2] >> t3
#[t1, t2, t3]
