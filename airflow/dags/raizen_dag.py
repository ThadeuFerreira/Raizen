from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from pathlib import Path 
import os
import SparkETL 
import pendulum
file_path = os.path.realpath(__file__)

local_tz = pendulum.timezone("America/Sao_Paulo")
default_args = {
    'owner': 'thadeu',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 10, tzinfo=local_tz),
    'email': ['thadeu.afm@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

pyspark_app_home = Path('/opt/airflow/dags/')

with DAG(dag_id="raizen_etl_dag",
         default_args=default_args,
         schedule_interval="@hourly",
         catchup=False) as dag:
         
        task1 = SparkSubmitOperator(task_id='load_excel_to_save_parquet',
            conn_id='spark_local',
            application=f'{pyspark_app_home}/SparkETL.py',
            total_executor_cores=4,
            executor_cores=2,
            executor_memory='5g',
            driver_memory='5g',
            name='load_excel_to_save_parquet',
            execution_timeout=timedelta(minutes=10),
            dag=dag
            )

        task1
