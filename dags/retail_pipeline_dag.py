"""
DAG : retail_sales_pipeline

Chaine : load_dimensions -> ingest_data -> validate_data
         -> transform_data -> load_warehouse
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingest import ingest_data
from src.quality import validate_data
from src.transform import transform_data
from src.load import load_dimensions, load_warehouse

default_args = {
    "owner": "taiss",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}

with DAG(
    dag_id="retail_sales_pipeline",
    description="Ingestion, qualite et chargement des ventes retail",
    default_args=default_args,
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["retail", "dwh", "taiss"],
) as dag:

    t0 = PythonOperator(task_id="load_dimensions", python_callable=load_dimensions)
    t1 = PythonOperator(task_id="ingest_data", python_callable=ingest_data)
    t2 = PythonOperator(task_id="validate_data", python_callable=validate_data)
    t3 = PythonOperator(task_id="transform_data", python_callable=transform_data)
    t4 = PythonOperator(task_id="load_warehouse", python_callable=load_warehouse)

    t0 >> t1 >> t2 >> t3 >> t4