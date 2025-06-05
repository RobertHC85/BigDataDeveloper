from airflow import DAG
#from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
#from airflow.operators.sensors import ExternalTaskSensor
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 1
dag1 = DAG(
    'dag_1',
    default_args=default_args,
    description='DAG 1',
    schedule_interval=timedelta(days=1),
)

start_task = EmptyOperator(
    task_id='start',
    dag=dag1,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag1,
)

start_task >> end_task

# DAG 2 (que depende de DAG 1)
dag2 = DAG(
    'dag_2',
    default_args=default_args,
    description='DAG 2',
    schedule_interval=timedelta(days=1),
)

start_task_dag2 = EmptyOperator(
    task_id='start_dag2',
    dag=dag2,
)

wait_for_dag1 = ExternalTaskSensor(
    task_id='wait_for_dag1',
    external_dag_id='dag_1',
    external_task_id='end',
    dag=dag2,
    mode='poke',
)

end_task_dag2 = EmptyOperator(
    task_id='end_dag2',
    dag=dag2,
)

start_task_dag2 >> wait_for_dag1 >> end_task_dag2
