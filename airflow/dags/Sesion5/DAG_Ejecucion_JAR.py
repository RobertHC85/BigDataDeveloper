from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Definir los argumentos por defecto
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2024, 10, 1),
}

# Definir el DAG
dag = DAG(
    'dag_run_java_jar',
    default_args=default_args,
    description='DAG que ejecuta un archivo .jar de Java',
    schedule_interval=timedelta(days=1),
)

# Operador de inicio vacío
start = EmptyOperator(task_id='start', dag=dag)

# Tarea Bash para compilar el archivo .java (opcional)
compile_java = BashOperator(
    task_id='compile_java',
    bash_command='javac /path/to/your/MainClass.java',
    dag=dag,
)

# Tarea Bash para ejecutar el archivo .jar
run_jar = BashOperator(
    task_id='run_jar_file',
    bash_command='java -jar /path/to/your/app.jar',
    dag=dag,
)

# Tarea Bash para procesar el resultado de la ejecución
process_results = BashOperator(
    task_id='process_results',
    bash_command='echo "Procesando los resultados..." && cat /path/to/your/output_file.txt',
    dag=dag,
)

# Operador de fin vacío
end = EmptyOperator(task_id='end', dag=dag)

# Definir dependencias
start >> compile_java >> run_jar >> process_results >> end
