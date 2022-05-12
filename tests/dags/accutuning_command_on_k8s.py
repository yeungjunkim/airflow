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


def make_env_var(input_dict):

    #     dict_str = "{{ dag_run.conf.accutuning_env_vars | tojson }}"
    #     # change_str = re.sub(r'(?<=[^\\])\\(?=[^\\])', r'\\\\', repr(dict_str)[1:-1])
    #     # print(change_str)
    #     change_str = json.loads(json.dumps(dict_str))
    #     # convert to dictionary
    #     # change_dict = json.loads(change_str)

    #     # dump_str = json.dumps(dict_str)
    #     # dict_str = json.loads(dump_str)
    #     # # json_str = json.loads(dict_str)
    #     # # clean_dict = json.dumps(dict_str)
    #     # change_dict = json.loads(dict_str)
    #     # env_dict_str = json.loads(kwargs['dag_run'].conf.accutuning_env_vars)

    env_dict = {
        'ACCUTUNING_WORKSPACE': input_dict.get("ACCUTUNING_WORKSPACE"),
        'ACCUTUNING_LOG_LEVEL': input_dict.get("ACCUTUNING_LOG_LEVEL"),
        'ACCUTUNING_USE_LABELER': input_dict.get("ACCUTUNING_USE_LABELER"),
        'ACCUTUNING_USE_CLUSTERING': input_dict.get("ACCUTUNING_USE_CLUSTERING"),
        'DJANGO_SETTINGS_MODULE': input_dict.get("DJANGO_SETTINGS_MODULE"),
        'ACCUTUNING_DB_ENGINE': input_dict.get("ACCUTUNING_DB_ENGINE"),
        'ACCUTUNING_DB_HOST': input_dict.get("ACCUTUNING_DB_HOST"),
        'ACCUTUNING_DB_PORT': input_dict.get("ACCUTUNING_DB_PORT"),
        'ACCUTUNING_DB_NAME': input_dict.get("ACCUTUNING_DB_NAME"),
        'ACCUTUNING_DB_USER': input_dict.get("ACCUTUNING_DB_USER"),
        'ACCUTUNING_DB_PASSWORD': input_dict.get("ACCUTUNING_DB_PASSWORD"),
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
        env_dict_str = json.loads(kwargs['context']['dag_run'].conf.get("accutuning_env_vars"))
        print(env_dict_str)
        print(type(env_dict_str))

        dict_str = "{{ dag_run.conf.accutuning_env_vars }}"
        print(dict_str)
        print(type(dict_str))
        change_str = json.dumps(dict_str).replace("'", '"')
        # change_str = json.dumps(dict_str)
        print(change_str)
        print(type(change_str))
        loads_str = json.loads(change_str)
        print(loads_str)
        print(type(loads_str))
        # eval_test = eval("echo '{{ dag_run.conf.accutuning_env_vars }}' ")
        # print(eval_test)
        # print(type(eval_test))
        # chg_eval_test = json.loads(eval_test)
        # print(chg_eval_test)
        # print(type(chg_eval_test))

        # print("+++++++++++++++++++++++++++")
        # print(kwargs['context']['dag_run'].conf.get("accutuning_env_vars"))
        # print(json.loads(kwargs['context']['dag_run'].conf.get("accutuning_env_vars")).get("ACCUTUNING_PVC_NAME"))
        # print(json.loads(kwargs['context']['dag_run'].conf.get("accutuning_env_vars")).get("ACCUTUNING_WORKSPACE"))
        # print("+++++++++++++++++++++++++++")

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
        self.env_vars = {
            'ACCUTUNING_WORKSPACE': env_dict_str.get("ACCUTUNING_WORKSPACE"),
            'ACCUTUNING_LOG_LEVEL': env_dict_str.get("ACCUTUNING_LOG_LEVEL"),
            'ACCUTUNING_USE_LABELER': env_dict_str.get("ACCUTUNING_USE_LABELER"),
            'ACCUTUNING_USE_CLUSTERING': env_dict_str.get("ACCUTUNING_USE_CLUSTERING"),
            'DJANGO_SETTINGS_MODULE': env_dict_str.get("DJANGO_SETTINGS_MODULE"),
            'ACCUTUNING_DB_ENGINE': env_dict_str.get("ACCUTUNING_DB_ENGINE"),
            'ACCUTUNING_DB_HOST': env_dict_str.get("ACCUTUNING_DB_HOST"),
            'ACCUTUNING_DB_PORT': env_dict_str.get("ACCUTUNING_DB_PORT"),
            'ACCUTUNING_DB_NAME': env_dict_str.get("ACCUTUNING_DB_NAME"),
            'ACCUTUNING_DB_USER': env_dict_str.get("ACCUTUNING_DB_USER"),
            'ACCUTUNING_DB_PASSWORD': env_dict_str.get("ACCUTUNING_DB_PASSWORD"),
        }
        # print("volume = {}".format(self.volume))
        return super().pre_execute(*args, **kwargs)


command_worker = KubernetesPodExOperator(
    namespace='default',
    # image='{{dag_run.conf.accutuning_env_vars.ACCUTUNING_APP_IMAGE}}',
    # image=json.loads("{{dag_run.conf.accutuning_env_vars}}").get("ACCUTUNING_APP_IMAGE"),
    # image=json.loads('{{dag_run.conf.accutuning_env_var}}').get("ACCUTUNING_APP_IMAGE"),
    image='pooh97/accu-app:k8s-9',
    # volumes=[volume],
    # volume_mounts=[volume_mount],
    name="monitor",
    task_id="monitor",
    # env_vars=make_env_var(),
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
