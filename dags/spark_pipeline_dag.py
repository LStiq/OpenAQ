from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Définition du DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spark_pipeline",
    default_args=default_args,
    description="Pipeline Spark pour analyser la pollution OpenAQ",
    schedule_interval="@daily",  # Exécuter tous les jours
    catchup=False
)

# Opérateur pour exécuter ton script Spark
run_spark_job = SparkSubmitOperator(
    task_id="run_spark_etl",
    application="/path/to/main.py",  # 📌 Remplace par le chemin de ton script
    conn_id="spark_default",
    dag=dag,
)

run_spark_job  # Ajoute la tâche au DAG
