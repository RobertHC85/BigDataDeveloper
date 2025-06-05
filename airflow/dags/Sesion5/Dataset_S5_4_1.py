from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

with DAG(
    dag_id='final_dag_ej2_1',
    start_date=datetime(2025, 6, 3),
    schedule_interval=None,  # Se puede lanzar manualmente o programar si deseas
    catchup=False,
) as dag:

    esperar_dag2 = ExternalTaskSensor(
        task_id='esperar_consumer_dag_S5_ej2_1',
        external_dag_id='consumer_dag_S5_ej2_1',
        external_task_id='consume_data_dag_S5_ej2_1',
        mode='poke',  # También puede ser 'reschedule'
        timeout=600,
        poke_interval=30,
        execution_delta=timedelta(minutes=0),  # Asegura que sea la misma ejecución
    )

    esperar_dag3 = ExternalTaskSensor(
        task_id='esperar_consumer_dag_S5_ej2_2',
        external_dag_id='consumer_dag_S5_ej2_2',
        external_task_id='consume_data_dag_S5_ej2_2',
        mode='poke',
        timeout=600,
        poke_interval=30,
        execution_delta=timedelta(minutes=0),
    )

    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    start >> [esperar_dag2, esperar_dag3] >> finish