#Crear las variables en Airflow:
#file_path: Ruta del archivo a procesar (ejemplo: /data/input_file.csv).
#max_rows: Número máximo de filas a procesar por ejecución (ejemplo: 1000).

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd

# Función para leer las variables y procesar el archivo
def etl_process(**kwargs):
    # Leer variables de Airflow
    file_path = Variable.get("file_path", default_var="/data/default_file.csv")
    max_rows = int(Variable.get("max_rows", default_var=50))  # Convertir a entero
    
    # Cargar el archivo CSV (con pandas en este caso)
    try:
        data = pd.read_csv(file_path)
        print(f"Archivo {file_path} cargado correctamente")
    except FileNotFoundError:
        print(f"Error: El archivo {file_path} no existe.")
        return

    # Limitar el número de filas a procesar
    data = data.head(max_rows)
    print(f"Procesando {len(data)} filas")

    # Supongamos que queremos imprimir la primera fila como ejemplo
    #print(data.iloc[0])
    if not data.empty:
        print(data.iloc[0])
    else:
        print("El DataFrame está vacío")

# Definir el DAG
with DAG(
    'etl_with_variables',
    start_date=datetime(2025, 6, 3),
    schedule_interval=None,
    catchup=False
) as dag:
    
    # Tarea que realiza el proceso ETL
    etl_task = PythonOperator(
        task_id='run_etl_process',
        python_callable=etl_process,
        provide_context=True,
    )
