from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from pathlib import Path 
import os

file_path = os.path.realpath(__file__)

def helloWorld():
    df = pd.DataFrame({'name': ['Raphael', 'Donatello'],
                   'mask': ['red', 'purple'],
                   'weapon': ['sai', 'bo staff']})
    print("File Path: " + file_path)
    filepath = Path('/opt/airflow/dags/out.csv')  
    filepath.parent.mkdir(parents=True, exist_ok=True)  
    df.to_csv(filepath)
    print('Hello World')

def readCSV():
    filepath = Path('/opt/airflow/dags/out.csv')  
    df = pd.read_csv(filepath)
    print('ReadCSV: ' + df['name'][0])

with DAG(dag_id="hello_world_dag",
         start_date=datetime(2021,1,1),
         schedule_interval="@hourly",
         catchup=False) as dag:
         
        task1 = PythonOperator(
                task_id="hello_world",
                python_callable=helloWorld)

        t2 = PythonOperator(
            task_id='task_readcsv',
            python_callable=readCSV,
            dag=dag)

        task1 >> t2