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

custom_env_vars = {}

dag = DAG(
    'accutuning_command_on_k8s', default_args=default_args, schedule_interval=None)

start = DummyOperator(task_id='start', dag=dag)


def make_accutuning_k8s_command(**kwargs):
    cmd = kwargs['dag_run'].conf['cmd']
    cmd_args = kwargs['dag_run'].conf['cmd_args']

    command = f'''/code/manage.py {cmd} '''
    command += '\n'.join([f'--{k}={v}' for (k, v) in cmd_args.items() if v])

    env_dict_str = kwargs['dag_run'].conf.get("accutuning_env_vars")
    env_dict = json.loads(env_dict_str)

    globals()["custom_env_vars"] = {
        "ACCUTUNING_WORKSPACE": env_dict.get("ACCUTUNING_WORKSPACE"),
        "ACCUTUNING_LOG_LEVEL": env_dict.get("ACCUTUNING_LOG_LEVEL"),
        "ACCUTUNING_USE_LABELER": env_dict.get("ACCUTUNING_USE_LABELER"),
        "ACCUTUNING_USE_CLUSTERING": env_dict.get("ACCUTUNING_USE_CLUSTERING"),
        "DJANGO_SETTINGS_MODULE": env_dict.get("DJANGO_SETTINGS_MODULE"),
        "ACCUTUNING_DB_ENGINE": env_dict.get("ACCUTUNING_DB_ENGINE"),
        "ACCUTUNING_DB_HOST": env_dict.get("ACCUTUNING_DB_HOST"),
        "ACCUTUNING_DB_PORT": env_dict.get("ACCUTUNING_DB_PORT"),
        "ACCUTUNING_DB_NAME": env_dict.get("ACCUTUNING_DB_NAME"),
        "ACCUTUNING_DB_USER": env_dict.get("ACCUTUNING_DB_USER"),
        "ACCUTUNING_DB_PASSWORD": env_dict.get("ACCUTUNING_DB_PASSWORD")
    }

    kwargs['task_instance'].xcom_push(key='command', value=command)
    # kwargs['task_instance'].xcom_push(key='env_dict', value=custom_env_vars)

    return command


def make_env_parameters():
    print(globals()["custom_env_vars"])
    print(type(globals()["custom_env_vars"]))
    return globals()["custom_env_vars"]


parameters = PythonOperator(task_id='make_parameters', python_callable=make_accutuning_k8s_command, dag=dag)

# template_fields: Sequence[str] = ('image', 'command', 'environment_str', 'container_name', 'volume_mount')


class KubernetesPodExOperator(KubernetesPodOperator):
    def __init__(self, *args, **kwargs):
        env_dict_str = json.loads(kwargs['context']['dag_run'].conf.get("accutuning_env_vars"))

        self.env_vars = {
            "ACCUTUNING_WORKSPACE": env_dict_str.get("ACCUTUNING_WORKSPACE"),
            "ACCUTUNING_LOG_LEVEL": env_dict_str.get("ACCUTUNING_LOG_LEVEL"),
            "ACCUTUNING_USE_LABELER": env_dict_str.get("ACCUTUNING_USE_LABELER"),
            "ACCUTUNING_USE_CLUSTERING": env_dict_str.get("ACCUTUNING_USE_CLUSTERING"),
            "DJANGO_SETTINGS_MODULE": env_dict_str.get("DJANGO_SETTINGS_MODULE"),
            "ACCUTUNING_DB_ENGINE": env_dict_str.get("ACCUTUNING_DB_ENGINE"),
            "ACCUTUNING_DB_HOST": env_dict_str.get("ACCUTUNING_DB_HOST"),
            "ACCUTUNING_DB_PORT": env_dict_str.get("ACCUTUNING_DB_PORT"),
            "ACCUTUNING_DB_NAME": env_dict_str.get("ACCUTUNING_DB_NAME"),
            "ACCUTUNING_DB_USER": env_dict_str.get("ACCUTUNING_DB_USER"),
            "ACCUTUNING_DB_PASSWORD": env_dict_str.get("ACCUTUNING_DB_PASSWORD")
        }
        super().__init__(*args, **kwargs)

    def pre_execute(self, *args, **kwargs):
        env_dict_str = json.loads(kwargs['context']['dag_run'].conf.get("accutuning_env_vars"))

        volume_mounts = k8s.V1VolumeMount(
            # name=kwargs['context']['dag_run'].conf.get("ACCUTUNING_PVC_NAME"),
            # mount_path=kwargs['context']['dag_run'].conf.get("ACCUTUNING_WORKSPACE"),
            # sub_path=None, read_only=False
            name=env_dict_str.get("ACCUTUNING_PVC_NAME"),
            mount_path=env_dict_str.get("ACCUTUNING_WORKSPACE"),
            sub_path=None, read_only=False
        )
        self.volume_mounts = [volume_mounts]

        volumes = k8s.V1Volume(
            # name=kwargs['context']['dag_run'].conf.get("ACCUTUNING_PVC_NAME"),
            # host_path=k8s.V1HostPathVolumeSource(path=kwargs['context']['dag_run'].conf.get("ACCUTUNING_WORKSPACE")),
            name=env_dict_str.get("ACCUTUNING_PVC_NAME"),
            host_path=k8s.V1HostPathVolumeSource(path=env_dict_str.get("ACCUTUNING_WORKSPACE")),
        )
        self.volumes = [volumes]
        self.arguments = kwargs['context']['task_instance'].xcom_pull(
            task_ids='make_parameters', key='command').split()
        self.image = str(env_dict_str.get("ACCUTUNING_APP_IMAGE"))

        return super().pre_execute(*args, **kwargs)

    def execute(self, *args, **kwargs):

        return super().execute(*args, **kwargs)


command_worker = KubernetesPodExOperator(
    namespace='default',
    name="monitor",
    task_id="monitor",
    # env_vars=make_env_parameters(),
    # env_vars=custom_env_vars,
    cmds=["python3"],
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
