from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Definir los argumentos por defecto
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 6, 3),
}

# Definir el DAG
dag = DAG(
    'ejemplo_bash_dag_s5',
    default_args=default_args,
    description='Un DAG que ejecuta múltiples tareas Bash',
    schedule_interval=timedelta(days=1),
)

# Operador de inicio vacío
start = EmptyOperator(task_id='start', dag=dag)

# Tarea Bash 1: Crear un directorio
create_dir = BashOperator(
    task_id='create_directory',
    bash_command='mkdir -p /tmp/my_dir_datapath_s5',
    dag=dag,
)

# Tarea Bash 2: Listar archivos en el directorio
list_files = BashOperator(
    task_id='list_files',
    bash_command='ls -la /tmp/my_dir_datapath_s5',
    dag=dag,
)

# Tarea Bash 3: Escribir en un archivo
write_file = BashOperator(
    task_id='write_to_file',
    bash_command='echo "Contenido del archivo" > /tmp/my_dir_datapath_s5/archivo.txt',
    dag=dag,
)

# Tarea Bash 4: Mostrar contenido del archivo
show_content = BashOperator(
    task_id='show_file_content',
    bash_command='cat /tmp/my_dir_datapath_s5/archivo.txt',
    dag=dag,
)

# Operador de fin vacío
end = EmptyOperator(task_id='end', dag=dag)

# Definir dependencias entre las tareas
start >> create_dir >> list_files >> write_file >> show_content >> end
