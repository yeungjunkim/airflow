from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'accutuning-worker-test', default_args=default_args, schedule_interval=timedelta(minutes=10))


start = DummyOperator(task_id='start', dag=dag)

worker = KubernetesPodOperator(namespace='default',
                          image="accutuning/modeler-common:latest",
#                           cmds=["python","-c"],
#                           arguments=["print('hello world')"],
                          labels={"ACCUTUNING_LOG_LEVEL": "INFO"},
                          name="accutuning-test",
                          task_id="accutuning",
                          get_logs=True,
                          dag=dag
                          )

end = DummyOperator(task_id='end', dag=dag)


worker.set_upstream(start)
worker.set_downstream(end)
