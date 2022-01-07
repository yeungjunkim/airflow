from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
  
def print_hello():
    return 'Hello world!'

dag = DAG('hello_world3', description='Simple tutorial DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task3', retries=3, dag=dag)

hello_operator = PythonOperator(task_id='hello_task3', python_callable=print_hello, dag=dag)

dummy_operator >> hello_operator
© 2022 GitHub, Inc.
Terms
Privacy
