from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 1
dag1 = DAG(
    'dag_1_ejemplo6',
    default_args=default_args,
    description='DAG 1',
    schedule_interval=timedelta(days=1),
)

start_task = DummyOperator(
    task_id='start',
    dag=dag1,
)

t_bash = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 10',
    dag=dag1,
)

trigger_dag2 = TriggerDagRunOperator(
    task_id='trigger_dag2',
    trigger_dag_id='dag_2_ejemplo6',
    dag=dag1,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag1,
)

start_task >> t_bash >> trigger_dag2 >> end_task

# DAG 2
dag2 = DAG(
    'dag_2_ejemplo6',
    default_args=default_args,
    description='DAG 2',
    schedule_interval=None,  # El DAG 2 solo se ejecuta cuando es disparado
)

start_task_dag2 = DummyOperator(
    task_id='start_dag2',
    dag=dag2,
)

t_bash_dag2 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag2,
)

end_task_dag2 = DummyOperator(
    task_id='end_dag2',
    dag=dag2,
)

start_task_dag2 >> t_bash_dag2 >> end_task_dag2
