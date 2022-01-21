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
    'kubernetes_accu_1', default_args=default_args, schedule_interval=timedelta(minutes=10))


start = DummyOperator(task_id='start', dag=dag)

passing = KubernetesPodOperator(namespace='default',
                          image="accutuning:latest",
#                           cmds=["python","-c"],
#                           arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="accutuning-test",
                          task_id="accutuning",
                          get_logs=True,
                          dag=dag
                          )

failing = KubernetesPodOperator(namespace='default',
                          #image="ubuntu:16.04",
                          image="accutuning/frontend:latest",
#                           cmds=["python","-c"],
#                           arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="fail",
                          task_id="accutuning_front-task",
                          get_logs=True,
                          dag=dag
                          )

end = DummyOperator(task_id='end', dag=dag)


passing.set_upstream(start)
failing.set_upstream(start)
passing.set_downstream(end)
failing.set_downstream(end)
