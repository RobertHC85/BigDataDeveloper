from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.hooks.base import BaseHook
import pymysql
import pandas as pd
import os

# Función para extraer datos
def extract_data():
    os.makedirs('/tmp/etl', exist_ok=True)

    conn = BaseHook.get_connection('my_db_connection_mysql')
    connection = pymysql.connect(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        database=conn.schema
    )
    query = "SELECT product_id, product_name, product_price FROM products"
    df = pd.read_sql(query, connection)
    df.to_csv('/tmp/etl/extracted_data.csv', index=False)
    connection.close()

# Función para transformar datos
def transform_data():
    df = pd.read_csv('/tmp/etl/extracted_data.csv')
    # Transformaciones de ejemplo
    df['product_price_por10'] = df['product_price'] * 10
    df.to_csv('/tmp/etl/transformed_data.csv', index=False)

# Función para cargar datos
def load_data():
    conn = BaseHook.get_connection('my_db_connection_mysql')
    connection = pymysql.connect(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        database=conn.schema
    )
    df = pd.read_csv('/tmp/etl/transformed_data.csv')
    cursor = connection.cursor()
    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO products_transformed (product_id, product_name, product_price_por10) VALUES (%s, %s, %s)", 
            (row['product_id'], row['product_name'], row['product_price_por10'])
        )
    connection.commit()
    cursor.close()
    connection.close()

# Argumentos por defecto
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='Un DAG simple para un proceso ETL',
    schedule_interval=timedelta(days=1),
)

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

start_task >> extract_task >> transform_task >> load_task >> end_task
