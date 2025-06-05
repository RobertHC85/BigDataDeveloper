from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base import BaseHook
import pymysql
import pandas as pd

# Función para extraer datos
def extract_data(table_name, output_path):
    conn = BaseHook.get_connection('my_db_connection')
    connection = pymysql.connect(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        database=conn.schema
    )
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, connection)
    df.to_csv(output_path, index=False)
    connection.close()

# Función para transformar datos
def transform_data(input_path, output_path):
    df = pd.read_csv(input_path)
    # Transformaciones de ejemplo
    df['new_column'] = df['existing_column'] * 2
    df.to_csv(output_path, index=False)

# Función para cargar datos
def load_data(input_path, table_name):
    conn = BaseHook.get_connection('my_db_connection')
    connection = pymysql.connect(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        database=conn.schema
    )
    df = pd.read_csv(input_path)
    cursor = connection.cursor()
    for _, row in df.iterrows():
        cursor.execute(
            f"INSERT INTO {table_name} (col1, col2) VALUES (%s, %s)", 
            (row['col1'], row['col2'])
        )
    connection.commit()
    cursor.close()
    connection.close()

# Argumentos por defecto
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
dag = DAG(
    'parallel_etl_dag',
    default_args=default_args,
    description='Un DAG simple para un proceso ETL con ejecución paralela',
    schedule_interval=timedelta(days=1),
)

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Tareas de extracción
extract_task_1 = PythonOperator(
    task_id='extract_data_table_1',
    python_callable=extract_data,
    op_args=['source_table_1', '/path/to/save/extracted_data_1.csv'],
    dag=dag,
)

extract_task_2 = PythonOperator(
    task_id='extract_data_table_2',
    python_callable=extract_data,
    op_args=['source_table_2', '/path/to/save/extracted_data_2.csv'],
    dag=dag,
)

# Tareas de transformación
transform_task_1 = PythonOperator(
    task_id='transform_data_table_1',
    python_callable=transform_data,
    op_args=['/path/to/save/extracted_data_1.csv', '/path/to/save/transformed_data_1.csv'],
    dag=dag,
)

transform_task_2 = PythonOperator(
    task_id='transform_data_table_2',
    python_callable=transform_data,
    op_args=['/path/to/save/extracted_data_2.csv', '/path/to/save/transformed_data_2.csv'],
    dag=dag,
)

# Tareas de carga
load_task_1 = PythonOperator(
    task_id='load_data_table_1',
    python_callable=load_data,
    op_args=['/path/to/save/transformed_data_1.csv', 'destination_table_1'],
    dag=dag,
)

load_task_2 = PythonOperator(
    task_id='load_data_table_2',
    python_callable=load_data,
    op_args=['/path/to/save/transformed_data_2.csv', 'destination_table_2'],
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Definir el flujo del DAG
start_task >> [extract_task_1, extract_task_2]
extract_task_1 >> transform_task_1 >> load_task_1 >> end_task
extract_task_2 >> transform_task_2 >> load_task_2 >> end_task
