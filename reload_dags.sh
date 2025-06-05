#!/bin/bash

echo "⏳ Recargando DAGs en Airflow..."

# Tocar un archivo para que Airflow detecte cambios en DAGs
docker exec bigdatadeveloper-airflow-webserver-1 bash -c "touch /opt/airflow/dags/__init__.py"

# Reiniciar el contenedor del scheduler
docker restart bigdatadeveloper-airflow-scheduler-1

echo "✅ DAGs recargados mediante reinicio del scheduler."
