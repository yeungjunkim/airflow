from airflow import DAG
from datetime import datetime, timedelta
# from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from kubernetes.client import models as k8s  # you should write this sentence when you could use volume, etc 
# from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.edgemodifier import Label  #label 쓰기 위한 library
import json
# from airflow.operators.dagrun_operator import TriggerDagRunOperator
# import pprint


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


configmaps = [
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='airflow-test-1')),
]


def make_uuid():
    import uuid
    return str(uuid.uuid4()).replace('-', '')


def make_accutuning_docker_command(django_command, experiment_id, container_uuid, execute_range, experiment_process_type, proceed_next, targets):
    command = f'''/code/manage.py {django_command}
--experiment={experiment_id} --uuid={container_uuid} --execute_range={execute_range}
--experiment_process_type={experiment_process_type} --proceed_next={proceed_next} '''
    return command + '\n'.join([f'--{k}={v}' for (k, v) in targets.items() if v])


def make_parameters(**kwargs):
    experiment_id = kwargs['dag_run'].conf['ACCUTUNING_EXPERIMENT_ID']
    experiment_process_type = kwargs['dag_run'].conf['experiment_process_type']
    proceed_next = kwargs['dag_run'].conf['proceed_next']
    use_ensemble = kwargs['dag_run'].conf['use_ensemble']
    container_uuid = make_uuid()
    django_command = experiment_process_type

    targets = dict(
        target_dataset=kwargs['dag_run'].conf.get('target_dataset'),
        target_dataset_eda=kwargs['dag_run'].conf.get('target_dataset_eda'),
        target_prediction=kwargs['dag_run'].conf.get('target_prediction'),
        target_model_base=kwargs['dag_run'].conf.get('target_model_base'),
        target_deployment=kwargs['dag_run'].conf.get('target_deployment'),
        target_source=kwargs['dag_run'].conf.get('target_source'),
    )

    command = make_accutuning_docker_command(django_command, experiment_id, container_uuid, '', experiment_process_type, proceed_next, targets)

    kwargs['task_instance'].xcom_push(key='command', value=command)

    print("experiment_id = {}".format(experiment_id))
    print("experiment_process_type = {}".format(experiment_process_type))
    print("use_ensemble = {}".format(use_ensemble))


def make_worker_env(**kwargs):
    workspace_path = kwargs['task_instance'].xcom_pull(task_ids='before_worker', key='return_value')["worker_workspace"]

    worker_env_vars_str = kwargs['dag_run'].conf['worker_env_vars']

    print(f'workspace_path:{workspace_path}')
    print(f'worker_env_vars:{worker_env_vars_str}')

    env_dict = json.loads(worker_env_vars_str)
    # env_dict = {}
    env_dict['ACCUTUNING_WORKSPACE'] = workspace_path
    env_dict['ACCUTUNING_LOG_LEVEL'] = kwargs['dag_run'].conf['ACCUTUNING_LOG_LEVEL']
    env_dict['ACCUTUNING_USE_LABELER'] = kwargs['dag_run'].conf['ACCUTUNING_USE_LABELER']
    env_dict['ACCUTUNING_USE_CLUSTERING'] = kwargs['dag_run'].conf['ACCUTUNING_USE_CLUSTERING']
    env_dict['DJANGO_SETTINGS_MODULE'] = kwargs['dag_run'].conf['DJANGO_SETTINGS_MODULE']

    kwargs['task_instance'].xcom_push(key='ACCUTUNING_WORKER_WORKSPACE', value=workspace_path)

    # worker_env_vars = json.dumps(env_dict)

    print(f'worker_env_vars:{env_dict}')

    kwargs['task_instance'].xcom_push(key='worker_env_vars', value=env_dict)


parameters = PythonOperator(task_id='make_parameters', python_callable=make_parameters, dag=dag)


class KubernetesPodExOperator(KubernetesPodOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def pre_execute(self, *args, **kwargs):
        volume_mounts = k8s.V1VolumeMount(
            name=kwargs['context']['dag_run'].conf.get('ACCUTUNING_PVC_NAME'),
            mount_path=kwargs['context']['dag_run'].conf.get('ACCUTUNING_WORKSPACE'),
            sub_path=None, read_only=False
        )
        self.volume_mounts = [volume_mounts]

        volumes = k8s.V1Volume(
            name=kwargs['context']['dag_run'].conf.get('ACCUTUNING_PVC_NAME'),
            # persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='test-volume', read_only=False),
            host_path=k8s.V1HostPathVolumeSource(path=kwargs['context']['dag_run'].conf.get('ACCUTUNING_WORKSPACE')),
        )
        self.volumes = [volumes]
        self.arguments = kwargs['context']['task_instance'].xcom_pull(
            task_ids='make_parameters', key='command').split()

        # print("volume = {}".format(self.volume))
        return super().pre_execute(*args, **kwargs)


before_worker = KubernetesPodExOperator(
    namespace='default',
    image='{{dag_run.conf.ACCUTUNING_APP_IMAGE}}',
    # image='pooh97/accu-app:latest',
    # volumes=[volume],
    # volume_mounts=[volume_mount],
    name="before_worker",
    task_id="before_worker",
    env_vars={
        'ACCUTUNING_WORKSPACE': '{{dag_run.conf.ACCUTUNING_WORKSPACE}}',
        'ACCUTUNING_LOG_LEVEL': '{{dag_run.conf.ACCUTUNING_LOG_LEVEL}}',
        'ACCUTUNING_USE_LABELER': '{{dag_run.conf.ACCUTUNING_USE_LABELER}}',
        'ACCUTUNING_USE_CLUSTERING': '{{dag_run.conf.ACCUTUNING_USE_CLUSTERING}}',
        'DJANGO_SETTINGS_MODULE': '{{dag_run.conf.DJANGO_SETTINGS_MODULE}}'
    },
    cmds=["python3"],

    do_xcom_push=True,
    image_pull_policy='Always',
    get_logs=True,
    dag=dag,
)


worker_env = PythonOperator(task_id='make_worker_env', python_callable=make_worker_env, dag=dag)


# one_success로 해야 skip된 task를 무시함
end = DummyOperator(
    task_id='end',
    trigger_rule='one_success',
    dag=dag,
)


start >> Label("parameter") >> parameters >> Label("app 중 before_worker Call") >> before_worker >> Label("common_module worker 중 Call") >> worker_env >> end
