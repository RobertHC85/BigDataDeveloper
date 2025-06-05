#DAG con BashOperator
#-------------------
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bash_dag',
    default_args=default_args,
    description='A simple bash DAG',
    schedule_interval=timedelta(days=1),
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 10',
    dag=dag,
)

t3 = BashOperator(
    task_id='print_hello',
    depends_on_past=False,
    bash_command='echo "Hello World!"',
    dag=dag,
)

#t1 >> t2 >> t3
t1 >> [t2,t3]
#t1 >> t2
#t1 >> t3