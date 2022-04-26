# from airflow.exceptions import AirflowSkipException, AirflowFailException, AirflowTaskTimeout
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from airflow.utils.state import State
# from airflow.api.common.mark_tasks import set_dag_run_state
# from airflow.api.common.experimental.mark_tasks import (
#     _create_dagruns,
#     set_dag_run_state_to_failed,
#     set_dag_run_state_to_running,
#     set_dag_run_state_to_success,
#     set_state,
# )
# from airflow.models import DagRun
# from airflow.utils import timezone
# from airflow.utils.dates import days_ago
# from airflow.utils.session import create_session, provide_session
# from airflow.utils.state import State
# from airflow.utils.types import DagRunType


def hello_world_py(*args, **kwargs):
    from pprint import pprint
    print('Hello World')
    pprint(args)
    pprint(kwargs)
    import time
    time.sleep(10)
    print('Good bye')


def check(*args, **kwargs):
    # TI = models.TaskInstance
    # with create_session() as session:
    #     tis = session.query(TI).filter(
    #         TI.dag_id == dag.dag_id,
    #         TI.execution_date.in_(execution_dates)
    #     ).all()
    for _ in range(5):
        for ti in kwargs["dag_run"].get_task_instances():
            # 각 task instance의 id와 state를 확인한다.
            task_id = ti.task_id
            state = ti.current_state()
            print(task_id, state)
        print('-' * 10)
        import time
        time.sleep(1)
    # print('set_state', set_dag_run_state_to_failed(
    #     dag=kwargs['dag_run'].dag,
    #     execution_date=kwargs['dag_run'].execution_date,
    #     # run_id=kwargs['dag_run'].run_id,
    #     commit=True))
    # raise AirflowTaskTimeout()
    for ti in kwargs["dag_run"].get_task_instances():
        ti.set_state(State.SKIPPED)
        # ti.set_state(State.FAILED)


dag_id = 'pilot_timeout'


default_args = {'owner': 'airflow',
                'start_date': datetime(2021, 1, 1)
                }

schedule = None

dag = DAG(
    dag_id,
    schedule_interval=schedule,
    default_args=default_args)

with dag:
    start = DummyOperator(task_id='start')
    t0 = DockerOperator(
        task_id='docker_test',
        image='busybox:latest',
        command=['sleep', '20'],
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker-ext.sock',
        network_mode='accutuning_default',
        mount_tmp_dir=False,
    )
    t1 = PythonOperator(
        task_id='hello_world01',
        provide_context=True,  # TODO: 차이점이 뭘까요?
        python_callable=hello_world_py)
    t2 = PythonOperator(
        task_id='hello_world02',
        provide_context=True,
        python_callable=hello_world_py)
    t3 = PythonOperator(
        task_id='hello_world03',
        provide_context=True,
        python_callable=hello_world_py)
    t4 = PythonOperator(
        task_id='hello_world04',
        provide_context=True,
        python_callable=hello_world_py)
    t5 = PythonOperator(
        task_id='hello_world05',
        provide_context=True,
        python_callable=hello_world_py)
    end = DummyOperator(task_id='end')
    start >> t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> end

    timer = PythonOperator(task_id='timer', provide_context=True, python_callable=check)
    start >> timer >> end
