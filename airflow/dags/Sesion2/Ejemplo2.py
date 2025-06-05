from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException

# Lista de días feriados
feriados = [
    datetime(2025, 1, 1),
    datetime(2025, 12, 25),
    datetime(2025, 5, 23),
    # Añadir otros días feriados aquí
]

def verificar_feriado(execution_date, **kwargs):
    if execution_date.date() in [feriado.date() for feriado in feriados]:
        raise AirflowSkipException("Hoy es un día feriado, saltando ejecución.")

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
    'dag_con_feriados',
    default_args=default_args,
    description='DAG que excluye días feriados',
    schedule_interval='0 0 * * MON-FRI',
)

verificar_feriado_task = PythonOperator(
    task_id='verificar_feriado',
    provide_context=True,
    python_callable=verificar_feriado,
    dag=dag,
)

# Aquí puedes definir tus otras tareas
tarea_ejemplo = PythonOperator(
    task_id='tarea_ejemplo',
    python_callable=lambda: print("Ejecutando tarea_ejemplo"),
    dag=dag,
)

verificar_feriado_task >> tarea_ejemplo
