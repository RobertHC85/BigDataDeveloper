from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.hooks.base import BaseHook
import pymysql
from airflow.operators.dummy_operator import DummyOperator

# Función que se ejecutará cuando se detecte el cambio en el flag
def execute_task():
    print("Flag detectado, ejecutando tarea...")

# Conexión a la base de datos
def get_db_conn():
    conn = BaseHook.get_connection('my_db_connection_mysql')
    return pymysql.connect(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        database=conn.schema
    )

# Definir los argumentos por defecto
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'flag_check_mysql_dag',
    default_args=default_args,
    description='DAG que verifica un flag y ejecuta una tarea si hay un cambio',
    schedule_interval=timedelta(minutes=2),
)

# Sensor para verificar el flag
flag_sensor = SqlSensor(
    task_id='check_flag',
    conn_id='my_db_connection_mysql',  # Este es el ID de la conexión configurada en Airflow
    sql='SELECT flag FROM my_table WHERE id = 1',  # Consulta para verificar el flag
    poke_interval=30,  # Intervalo de verificación en segundos (2 minutos)
    mode='poke',
    dag=dag,
)

# Operador que ejecuta la tarea cuando se detecta el cambio en el flag
task_executor = PythonOperator(
    task_id='execute_task',
    python_callable=execute_task,
    dag=dag,
)

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Definir el flujo del DAG
start_task >> flag_sensor >> task_executor >> end_task
