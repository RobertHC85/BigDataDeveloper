#Crear las variables en Airflow:
#file_path: Ruta del archivo a procesar (ejemplo: /data/input_file.csv).
#max_rows: Número máximo de filas a procesar por ejecución (ejemplo: 1000).

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import os

# Función para leer las variables y procesar el archivo
def etl_process(**kwargs):
    #Extracción
    # Leer variables de Airflow
    file_path = Variable.get("file_path", default_var="/data/default_file.csv")
    max_rows = int(Variable.get("max_rows", default_var=50))  # Convertir a entero
    
    if not os.path.exists(file_path):
        print(f"El archivo no existe: {file_path}")
        return    

    # Cargar el archivo CSV (con pandas en este caso)
    try:
        data = pd.read_csv(file_path)
        print(f"Archivo cargado correctamente: {file_path}")
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return

    # Limitar el número de filas a procesar
    data = data.head(max_rows)
    print(f"Procesando {len(data)} filas")

    # 2. TRANSFORM
    # Elimina columnas vacías o con todos los valores nulos
    data = data.dropna(axis=1, how='all')

    # Renombra columnas si lo deseas
    data.columns = [col.strip().lower().replace(" ", "_") for col in data.columns]    

    # Crea una nueva columna ejemplo
    if "sensor1" in data.columns and "sensor2" in data.columns:
        data["promedio_sensores"] = (data["sensor1"] + data["sensor2"]) / 2

    print(f"Datos transformados: {len(data)} filas")

    #Load
    output_path = "/opt/airflow/dags/Sesion5/data/output.csv"
    try:
        data.to_csv(output_path, index=False)
        print(f"Archivo de salida guardado en: {output_path}")
    except Exception as e:
        print(f"Error al guardar el archivo de salida: {e}")    

    # Supongamos que queremos imprimir la primera fila como ejemplo
    #print(data.iloc[0])
    if not data.empty:
        print(data.iloc[0])
    else:
        print("El DataFrame está vacío")

# Definir el DAG
with DAG(
    'etl_with_variables_1',
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
