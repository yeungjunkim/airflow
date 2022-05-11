from airflow import DAG
from datetime import datetime, timedelta
# from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from kubernetes.client import models as k8s  # you should write this sentence when you could use volume, etc 
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

dag = DAG(
    'accutuning_command_on_k8s', default_args=default_args, schedule_interval=None)

start = DummyOperator(task_id='start', dag=dag)


def make_env_var(**kwargs):

    # env_vars_dict = json.loads(self.conf['accutuning_env_vars'])['ACCUTUNING_K8S_USE']
    env_vars_dict = json.loads(kwargs['dag_run'].conf.accutuning_env_vars)
    print(env_vars_dict)
    env_dict = {
        # 'ACCUTUNING_WORKSPACE': '{{dag_run.conf.ACCUTUNING_WORKSPACE}}',
        # 'ACCUTUNING_LOG_LEVEL': '{{dag_run.conf.ACCUTUNING_LOG_LEVEL}}',
        # 'ACCUTUNING_USE_LABELER': '{{dag_run.conf.ACCUTUNING_USE_LABELER}}',
        # 'ACCUTUNING_USE_CLUSTERING': '{{dag_run.conf.ACCUTUNING_USE_CLUSTERING}}',
        # 'DJANGO_SETTINGS_MODULE': '{{dag_run.conf.DJANGO_SETTINGS_MODULE}}',
        # 'ACCUTUNING_DB_ENGINE': '{{dag_run.conf.ACCUTUNING_DB_ENGINE}}',
        # 'ACCUTUNING_DB_HOST': '{{dag_run.conf.ACCUTUNING_DB_HOST}}',
        # 'ACCUTUNING_DB_PORT': '{{dag_run.conf.ACCUTUNING_DB_PORT}}',
        # 'ACCUTUNING_DB_NAME': '{{dag_run.conf.ACCUTUNING_DB_NAME}}',
        # 'ACCUTUNING_DB_USER': '{{dag_run.conf.ACCUTUNING_DB_USER}}',
        # 'ACCUTUNING_DB_PASSWORD': '{{dag_run.conf.ACCUTUNING_DB_PASSWORD}}',
        # 'ACCUTUNING_WORKSPACE': env_vars_dict.get("ACCUTUNING_WORKSPACE"),
        # 'ACCUTUNING_LOG_LEVEL': env_vars_dict.get("ACCUTUNING_LOG_LEVEL"),
        # 'ACCUTUNING_USE_LABELER': env_vars_dict.get("ACCUTUNING_USE_LABELER"),
        # 'ACCUTUNING_USE_CLUSTERING': env_vars_dict.get("ACCUTUNING_USE_CLUSTERING"),
        # 'DJANGO_SETTINGS_MODULE': env_vars_dict.get("DJANGO_SETTINGS_MODULE"),
        # 'ACCUTUNING_DB_ENGINE': env_vars_dict.get("ACCUTUNING_DB_ENGINE"),
        # 'ACCUTUNING_DB_HOST': env_vars_dict.get("ACCUTUNING_DB_HOST"),
        # 'ACCUTUNING_DB_PORT': env_vars_dict.get("ACCUTUNING_DB_PORT"),
        # 'ACCUTUNING_DB_NAME': env_vars_dict.get("ACCUTUNING_DB_NAME"),
        # 'ACCUTUNING_DB_USER': env_vars_dict.get("ACCUTUNING_DB_USER"),
        # 'ACCUTUNING_DB_PASSWORD': env_vars_dict.get("ACCUTUNING_DB_PASSWORD"),
        'ACCUTUNING_WORKSPACE': env_vars_dict.get("ACCUTUNING_WORKSPACE"),
        'ACCUTUNING_LOG_LEVEL': env_vars_dict.get("ACCUTUNING_LOG_LEVEL"),
        'ACCUTUNING_USE_LABELER': env_vars_dict.get("ACCUTUNING_USE_LABELER"),
        'ACCUTUNING_USE_CLUSTERING': env_vars_dict.get("ACCUTUNING_USE_CLUSTERING"),
        'DJANGO_SETTINGS_MODULE': env_vars_dict.get("DJANGO_SETTINGS_MODULE"),
        'ACCUTUNING_DB_ENGINE': env_vars_dict.get("ACCUTUNING_DB_ENGINE"),
        'ACCUTUNING_DB_HOST': env_vars_dict.get("ACCUTUNING_DB_HOST"),
        'ACCUTUNING_DB_PORT': env_vars_dict.get("ACCUTUNING_DB_PORT"),
        'ACCUTUNING_DB_NAME': env_vars_dict.get("ACCUTUNING_DB_NAME"),
        'ACCUTUNING_DB_USER': env_vars_dict.get("ACCUTUNING_DB_USER"),
        'ACCUTUNING_DB_PASSWORD': env_vars_dict.get("ACCUTUNING_DB_PASSWORD"),
    }
    return env_dict


def make_accutuning_k8s_command(**kwargs):
    cmd = kwargs['dag_run'].conf['cmd']
    cmd_args = kwargs['dag_run'].conf['cmd_args']

    command = f'''/code/manage.py {cmd} '''
    command += '\n'.join([f'--{k}={v}' for (k, v) in cmd_args.items() if v])

    kwargs['task_instance'].xcom_push(key='command', value=command)

    return command


parameters = PythonOperator(task_id='make_parameters', python_callable=make_accutuning_k8s_command, dag=dag)


class KubernetesPodExOperator(KubernetesPodOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def pre_execute(self, *args, **kwargs):
        env_vars_dict = json.loads(kwargs['dag_run'].conf.accutuning_env_vars)

        volume_mounts = k8s.V1VolumeMount(
            # name=kwargs['context']['dag_run'].conf.get("ACCUTUNING_PVC_NAME"),
            # mount_path=kwargs['context']['dag_run'].conf.get("ACCUTUNING_WORKSPACE"),
            # sub_path=None, read_only=False
            name=env_vars_dict.get("ACCUTUNING_PVC_NAME"),
            mount_path=env_vars_dict.get("ACCUTUNING_WORKSPACE"),
            sub_path=None, read_only=False
        )
        self.volume_mounts = [volume_mounts]

        volumes = k8s.V1Volume(
            # name=kwargs['context']['dag_run'].conf.get("ACCUTUNING_PVC_NAME"),
            # host_path=k8s.V1HostPathVolumeSource(path=kwargs['context']['dag_run'].conf.get("ACCUTUNING_WORKSPACE")),
            name=env_vars_dict.get("ACCUTUNING_PVC_NAME"),
            host_path=k8s.V1HostPathVolumeSource(path=env_vars_dict.get("ACCUTUNING_WORKSPACE")),
        )
        self.volumes = [volumes]
        self.arguments = kwargs['context']['task_instance'].xcom_pull(
            task_ids='make_parameters', key='command').split()

        # print("volume = {}".format(self.volume))
        return super().pre_execute(*args, **kwargs)


command_worker = KubernetesPodExOperator(
    namespace='default',
    image='{{dag_run.conf.ACCUTUNING_APP_IMAGE}}',
    # image=json.loads("{{dag_run.conf.accutuning_env_vars}}").get("ACCUTUNING_APP_IMAGE"),
    # image='pooh97/accu-app:latest',
    # volumes=[volume],
    # volume_mounts=[volume_mount],
    name="monitor",
    task_id="monitor",
    env_vars=make_env_var(),
    cmds=["python3"],

    do_xcom_push=True,
    image_pull_policy='Always',
    get_logs=True,
    dag=dag,
)

# one_success로 해야 skip된 task를 무시함
end = DummyOperator(
    task_id='end',
    trigger_rule='one_success',
    dag=dag,
)

start >> parameters >> command_worker >> end
