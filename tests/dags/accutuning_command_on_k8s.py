from airflow import DAG
from datetime import datetime, timedelta
# from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from kubernetes.client import models as k8s  # you should write this sentence when you could use volume, etc 
from airflow.utils.state import State
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'render_template_as_native_obj': True,
    'provide_context': True,
}

custom_env_vars = {}

dag = DAG(
    'accutuning_command_on_k8s', default_args=default_args, max_active_runs=2, schedule_interval=None)

start = DummyOperator(task_id='start', dag=dag)


def make_accutuning_k8s_command(**kwargs):
    cmd = kwargs['dag_run'].conf['cmd']
    cmd_args = kwargs['dag_run'].conf['cmd_args']

    command = f'''/code/manage.py {cmd} '''
    command += '\n'.join([f'--{k}={v}' for (k, v) in cmd_args.items() if v])

    env_dict_str = kwargs['dag_run'].conf.get("accutuning_env_vars")
    env_dict = json.loads(env_dict_str)

    kwargs['task_instance'].xcom_push(key='command', value=command)
    for (k, v) in env_dict.items():
        kwargs['task_instance'].xcom_push(key=k, value=v)

    return command


def make_env_parameters(**kwargs):
    print(kwargs)
    print(type(kwargs))

    # env_dict = ti.xcom_pull(task_id='make_parameters')

    # print(env_dict)
    # print(type(env_dict))

    return {}


def _check(*args, **kwargs):

    import time
    process_default_timeout = 600
    max_eval_time = kwargs['dag_run'].conf.get('experiment_config', {}).get('experiment', {}).get('max_eval_time', process_default_timeout)
    estimator_dict = json.loads(kwargs['dag_run'].conf.get('experiment_config', {}).get('experiment', {}).get('include_estimators_json'))
    estimator_cnt = 0

    # estimator ?????? ????????? optuna??? ???????????? ???????????? ?????? ?????????????????? ??????
    if len(estimator_dict) == 0:
        estimator_cnt = 1
        timeout = (max_eval_time * estimator_cnt)
    else:
        estimator_cnt = len(estimator_dict)
        timeout = (max_eval_time * estimator_cnt / 3)

    print(f'estimator_dict = [{estimator_dict}]')
    print(f'process_default_timeout = [{process_default_timeout}]')
    print(f'estimator_cnt = [{estimator_cnt}]')
    print(f'max_eval_time = [{max_eval_time}]')
    print(f'process_default_timeout = [{process_default_timeout}]')
    print(f'timeout = [{timeout}]')

    time_count = 1

    while time_count < timeout:
        time.sleep(1)
        time_count += 1

        # task_id = kwargs["dag_run"].get_task_instance('end').task_id
        end_state = kwargs["dag_run"].get_task_instance('end').current_state()

        if end_state in ["success", "failed"]:
            return True

    for ti in kwargs["dag_run"].get_task_instances():
        if ti.current_state() in ('running', None):
            if ti.task_id not in ('worker_success', 'worker_fail', 'end'):
                print(f'ti.task_id = {ti.task_id}')
                ti.set_state(State.FAILED)


parameters = PythonOperator(task_id='make_parameters', python_callable=make_accutuning_k8s_command, provide_context=True, dag=dag)

# template_fields: Sequence[str] = ('image', 'command', 'environment_str', 'container_name', 'volume_mount')


class KubernetesPodExOperator(KubernetesPodOperator):
    # from typing import Sequence
    # template_fields: Sequence[str] = (
    #     'image',
    #     'cmds',
    #     'arguments',
    #     'env_vars',
    #     'labels',
    #     'config_file',
    #     'pod_template_file',
    #     'namespace',
    # )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def pre_execute(self, *args, **kwargs):
        env_dict_str = json.loads(kwargs['context']['dag_run'].conf.get("accutuning_env_vars"))

        # for pvc (do not remove this code)
        # -----------------------------------
        volume_mounts = k8s.V1VolumeMount(
            name=env_dict_str.get('ACCUTUNING_PVC_NAME'),
            mount_path=env_dict_str.get('ACCUTUNING_WORKSPACE'),
            sub_path=None, read_only=False
        )
        self.volume_mounts = [volume_mounts]

        volumes = k8s.V1Volume(
            name=env_dict_str.get('ACCUTUNING_PVC_NAME'),
            # persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='test-volume', read_only=False),
            host_path=k8s.V1HostPathVolumeSource(path=env_dict_str.get('ACCUTUNING_WORKSPACE')),
        )
        self.volumes = [volumes]
        # -----------------------------------

        # for nfs
        # -----------------------------------
        # volumes = k8s.V1Volume(
        #     name='accutuning-workspace',
        #     nfs=k8s.V1NFSVolumeSource(
        #         path=env_dict_str.get('ACCUTUNING_K8S_VOLUME_MOUNT_PATH'),
        #         # server=env_dict_str.get('ACCUTUNING_K8S_VOLUME_MOUNT_SERVER'),
        #         server="fs-a43ef4c4.efs.ap-northeast-2.amazonaws.com",
        #         readOnly=False,
        #     )
        # )
        # self.volumes = [volumes]

        # volume_mounts = k8s.V1VolumeMount(
        #     name='accutuning-workspace',
        #     mount_path=env_dict_str.get('ACCUTUNING_WORKSPACE'),
        #     sub_path=None, read_only=False
        # )
        # self.volume_mounts = [volume_mounts]
        # -----------------------------------

        self.arguments = kwargs['context']['task_instance'].xcom_pull(
            task_ids='make_parameters', key='command').split()
        self.image = str(env_dict_str.get("ACCUTUNING_APP_IMAGE"))

        return super().pre_execute(*args, **kwargs)

    def execute(self, *args, **kwargs):
        # env_dict_str = json.loads(kwargs['context']['dag_run'].conf.get("accutuning_env_vars"))

        # self.env_vars = {
        #     "ACCUTUNING_WORKSPACE": env_dict_str.get("ACCUTUNING_WORKSPACE"),
        #     "ACCUTUNING_LOG_LEVEL": env_dict_str.get("ACCUTUNING_LOG_LEVEL"),
        #     "ACCUTUNING_USE_LABELER": env_dict_str.get("ACCUTUNING_USE_LABELER"),
        #     "ACCUTUNING_USE_CLUSTERING": env_dict_str.get("ACCUTUNING_USE_CLUSTERING"),
        #     "DJANGO_SETTINGS_MODULE": env_dict_str.get("DJANGO_SETTINGS_MODULE"),
        #     "ACCUTUNING_DB_ENGINE": env_dict_str.get("ACCUTUNING_DB_ENGINE"),
        #     "ACCUTUNING_DB_HOST": env_dict_str.get("ACCUTUNING_DB_HOST"),
        #     "ACCUTUNING_DB_PORT": env_dict_str.get("ACCUTUNING_DB_PORT"),
        #     "ACCUTUNING_DB_NAME": env_dict_str.get("ACCUTUNING_DB_NAME"),
        #     "ACCUTUNING_DB_USER": env_dict_str.get("ACCUTUNING_DB_USER"),
        #     "ACCUTUNING_DB_PASSWORD": env_dict_str.get("ACCUTUNING_DB_PASSWORD")
        # }

        return super().execute(*args, **kwargs)


command_worker = KubernetesPodExOperator(
    namespace='default',
    name="monitor",
    task_id="monitor",
    env_vars={
        "ACCUTUNING_WORKSPACE": "{{ti.xcom_pull(key='ACCUTUNING_WORKSPACE', task_ids='make_parameters') }}",
        "ACCUTUNING_LOG_LEVEL": "{{ti.xcom_pull(key='ACCUTUNING_LOG_LEVEL', task_ids='make_parameters') }}",
        "ACCUTUNING_USE_LABELER": "{{ti.xcom_pull(key='ACCUTUNING_USE_LABELER', task_ids='make_parameters') }}",
        "ACCUTUNING_USE_CLUSTERING": "{{ti.xcom_pull(key='ACCUTUNING_USE_CLUSTERING', task_ids='make_parameters') }}",
        "DJANGO_SETTINGS_MODULE": "{{ti.xcom_pull(key='DJANGO_SETTINGS_MODULE', task_ids='make_parameters') }}",
        "ACCUTUNING_DB_ENGINE": "{{ti.xcom_pull(key='ACCUTUNING_DB_ENGINE', task_ids='make_parameters') }}",
        "ACCUTUNING_DB_HOST": "{{ti.xcom_pull(key='ACCUTUNING_DB_HOST', task_ids='make_parameters') }}",
        "ACCUTUNING_DB_PORT": "{{ti.xcom_pull(key='ACCUTUNING_DB_PORT', task_ids='make_parameters') }}",
        "ACCUTUNING_DB_NAME": "{{ti.xcom_pull(key='ACCUTUNING_DB_NAME', task_ids='make_parameters') }}",
        "ACCUTUNING_DB_USER": "{{ti.xcom_pull(key='ACCUTUNING_DB_USER', task_ids='make_parameters') }}",
        "ACCUTUNING_DB_PASSWORD": "{{ti.xcom_pull(key='ACCUTUNING_DB_PASSWORD', task_ids='make_parameters') }}",
    },
    cmds=["python3"],
    image_pull_policy='Always',
    get_logs=True,
    dag=dag,
)

# one_success??? ?????? skip??? task??? ?????????
end = DummyOperator(
    task_id='end',
    trigger_rule='one_success',
    dag=dag,
)

timer = PythonOperator(task_id='timer', provide_context=True, python_callable=_check, dag=dag)


start >> parameters >> command_worker >> end

start >> timer >> end
