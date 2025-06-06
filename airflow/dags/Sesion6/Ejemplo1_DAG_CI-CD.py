#Ejemplo2 de CI/CD
#Nueva configuración del yml en GitHub
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="DAG_ci_cd_prueba",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    description="DAG de prueba para validar CI/CD en Codespace",
    tags=["ci_cd", "prueba"],
) as dag:

    tarea_prueba = BashOperator(
        task_id="tarea_de_prueba",
        bash_command="echo 'CI/CD ejecutado correctamente en DAG_ci_cd_prueba'",
    )