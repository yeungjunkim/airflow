from airflow import DAG
from datetime import datetime, timedelta
# from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from kubernetes.client import models as k8s  # you should write this sentence when you could use volume, etc
from airflow.utils.state import State
import json
# from airflow.operators.dagrun_operator import TriggerDagRunOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'render_template_as_native_obj': True,
    'provide_context': True,
}
dag = DAG(
    'ml_run_k8s', default_args=default_args, max_active_runs=2, schedule_interval=None)
# dag = DAG(
#     'ml_run_k8s', default_args=default_args, schedule_interval=None)

start = DummyOperator(task_id='start', dag=dag)


# configmaps = [
#     k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='airflow-test-1')),
# ]


def make_uuid():
    import uuid
    return str(uuid.uuid4()).replace('-', '')


def get_command_name(experiment_process_type):
    command_dict = {  # TODO 이 참에 이거 전부 통일하면 안될까? ml_ + process_type
        'parse': 'ml_parse',
        'labeling': 'lb_tagtext',
        'lb_predict': 'lb_predict',
        'preprocess': 'ml_preprocess',
        'optuna': 'ml_optuna',
        'modelstat': 'ml_modelstat',
        'deploy': 'ml_deploy',
        'predict': 'ml_predict',
        'ensemble': 'ml_ensemble',
        'cluster': 'cl_run',
        'cl_predict': 'cl_predict',
        'dataset_eda': 'ml_dataset_eda',
        'optuna_monitor': 'ml_optuna_monitor',
        'ensemble_monitor': 'ml_ensemble_monitor',
    }
    return command_dict[experiment_process_type]


def make_accutuning_docker_command(django_command, experiment_id, container_uuid, execute_range, experiment_process_type, proceed_next, triggered_dag_id, triggered_dag_run_id, airflow_dag_call_id, estimators, targets):
    command = f'''/code/manage.py {django_command}
--experiment={experiment_id} --uuid={container_uuid} --execute_range={execute_range}
--experiment_process_type={experiment_process_type} --proceed_next={proceed_next}
--triggered_dag_id={triggered_dag_id} --triggered_dag_run_id={triggered_dag_run_id}
--airflow_dag_call_id={airflow_dag_call_id}'''
    if estimators:
        command += f' --estimators={estimators}'
    return command + ' ' + '\n'.join([f'--{k}={v}' for (k, v) in targets.items() if v])


def make_parameters(**kwargs):
    experiment_id = kwargs['dag_run'].conf.get('experiment_id')
    experiment_process_type = kwargs['dag_run'].conf.get('experiment_process_type')
    proceed_next = kwargs['dag_run'].conf.get('proceed_next')
    estimators = kwargs['dag_run'].conf.get('estimators')
    targets = dict(
        target_dataset=kwargs['dag_run'].conf.get('target_dataset'),
        target_dataset_eda=kwargs['dag_run'].conf.get('target_dataset_eda'),
        target_prediction=kwargs['dag_run'].conf.get('target_prediction'),
        target_model_base=kwargs['dag_run'].conf.get('target_model_base'),
        target_deployment=kwargs['dag_run'].conf.get('target_deployment'),
        target_source=kwargs['dag_run'].conf.get('target_source'),
    )
    triggered_dag_id = 'ml_run_k8s'
    triggered_dag_run_id = kwargs['dag_run'].run_id
    airflow_dag_call_id = kwargs['dag_run'].conf['airflow_dag_call_id']

    container_uuid = make_uuid()
    django_command = get_command_name(experiment_process_type)
    docker_command_before = make_accutuning_docker_command(django_command, experiment_id, container_uuid, 'before', experiment_process_type, proceed_next, triggered_dag_id, triggered_dag_run_id, airflow_dag_call_id, estimators, targets)
    docker_command_after = make_accutuning_docker_command(django_command, experiment_id, container_uuid, 'after', experiment_process_type, proceed_next, triggered_dag_id, triggered_dag_run_id, airflow_dag_call_id, estimators, targets)

    env_dict = json.loads(kwargs['dag_run'].conf.get("accutuning_env_vars"))
    for (k, v) in env_dict.items():
        kwargs['task_instance'].xcom_push(key=k, value=v)

    kwargs['task_instance'].xcom_push(key='app_env_vars', value=kwargs['dag_run'].conf.get('app_env_vars'))
    kwargs['task_instance'].xcom_push(key='before_command', value=docker_command_before)
    kwargs['task_instance'].xcom_push(key='after_command', value=docker_command_after)


def make_worker_env(**kwargs):
    workspace_path = kwargs['task_instance'].xcom_pull(task_ids='before_worker', key='return_value')["worker_workspace"]
    resources_str = kwargs['task_instance'].xcom_pull(task_ids='before_worker', key='return_value')["worker_resources"]
    worker_namespace = kwargs['task_instance'].xcom_pull(task_ids='before_worker', key='return_value')["worker_namespace"]

    kwargs['task_instance'].xcom_push(key='ACCUTUNING_WORKER_WORKSPACE', value=workspace_path)
    kwargs['task_instance'].xcom_push(key='worker_namespace', value=worker_namespace)
    kwargs['task_instance'].xcom_push(key='resources_str', value=resources_str)  # k8s resources_str


def make_env_var():
    env_dict = {
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
    }
    return env_dict


parameters = PythonOperator(task_id='make_parameters', python_callable=make_parameters, dag=dag)


def _check(*args, **kwargs):

    # for _ in range(len(kwargs["dag_run"].get_task_instances())):
    #     for ti in kwargs["dag_run"].get_task_instances():
    #         # 각 task instance의 id와 state를 확인한다.
    #         task_id = ti.task_id
    #         state = ti.current_state()
    #         print(task_id, state)

    import time
    process_default_timeout = 600
    timeout = kwargs['dag_run'].conf.get('experiment_config', {}).get('experiment', {}).get('max_eval_time', process_default_timeout)
    if timeout == {}:
        timeout = process_default_timeout
    print(f'timeout = [{timeout}]')

    time_count = 1

    while time_count < timeout:
        time.sleep(1)
        time_count += 1

        # make_worker_env_state = kwargs["dag_run"].get_task_instance('make_worker_env').current_state()
        # if make_worker_env_state == "failed":
        #     return True

        before_worker_state = kwargs["dag_run"].get_task_instance('before_worker').current_state()
        if before_worker_state == "failed":
            return True

        worker_state = kwargs["dag_run"].get_task_instance('worker').current_state()
        if worker_state == "failed":
            return True

        # task_id = kwargs["dag_run"].get_task_instance('end').task_id
        end_state = kwargs["dag_run"].get_task_instance('end').current_state()

        if end_state in ["success", "failed"]:
            return True

    for ti in kwargs["dag_run"].get_task_instances():
        if ti.current_state() in ('running', None):
            if ti.task_id not in ('worker_success', 'worker_fail', 'end'):
                print(f'ti.task_id = {ti.task_id}')
                ti.set_state(State.FAILED)


class KubernetesPodExOperator(KubernetesPodOperator):
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

        self.image_pull_policy = env_dict_str.get('ACCUTUNING_K8S_IMAGE_PULL_POLICY')
        self.image_pull_secrets = [k8s.V1LocalObjectReference(env_dict_str.get('ACCUTUNING_K8S_IMAGE_PULL_SECRET'))]
        if env_dict_str.get('ACCUTUNING_K8S_NODETYPE'):
            self.node_selector = {'node_type': env_dict_str.get('ACCUTUNING_K8S_NODETYPE')}

        return super().pre_execute(*args, **kwargs)


class KubernetesPodExPreOperator(KubernetesPodExOperator):
    def pre_execute(self, *args, **kwargs):
        env_dict_str = json.loads(kwargs['context']['dag_run'].conf.get("accutuning_env_vars"))
        self.arguments = kwargs['context']['task_instance'].xcom_pull(
            task_ids='make_parameters', key='before_command').split()
        self.image = str(env_dict_str.get("ACCUTUNING_APP_IMAGE"))
        return super().pre_execute(*args, **kwargs)


class KubernetesPodExPostOperator(KubernetesPodExOperator):
    def pre_execute(self, *args, **kwargs):
        env_dict_str = json.loads(kwargs['context']['dag_run'].conf.get("accutuning_env_vars"))
        self.arguments = kwargs['context']['task_instance'].xcom_pull(
            task_ids='make_parameters', key='after_command').split()
        self.image = str(env_dict_str.get("ACCUTUNING_APP_IMAGE"))
        return super().pre_execute(*args, **kwargs)


class KubernetesPodExWorkerOperator(KubernetesPodExOperator):
    def pre_execute(self, *args, **kwargs):
        env_dict_str = json.loads(kwargs['context']['dag_run'].conf.get("accutuning_env_vars"))
        self.resources = k8s.V1ResourceRequirements(
            limits=json.loads(kwargs['context']['task_instance'].xcom_pull(
                task_ids='make_worker_env', key='resources_str'))
        )
        self.image = str(env_dict_str.get("ACCUTUNING_WORKER_IMAGE"))
        return super().pre_execute(*args, **kwargs)


before_worker = KubernetesPodExPreOperator(
    namespace='{{ ti.xcom_pull(key="ACCUTUNING_K8S_WORKER_NAMESPACE")}}',
    name="before_worker",
    task_id="before_worker",
    env_vars=make_env_var(),
    cmds=["python3"],
    do_xcom_push=True,
    get_logs=True,
    dag=dag,
)


worker_env = PythonOperator(task_id='make_worker_env', python_callable=make_worker_env, dag=dag)

worker = KubernetesPodExWorkerOperator(
    namespace='{{ ti.xcom_pull(key="ACCUTUNING_K8S_WORKER_NAMESPACE")}}',
    # image="{{dag_run.conf.ACCUTUNING_WORKER_IMAGE}}",
    name="worker",
    task_id="worker",
    env_vars={'ACCUTUNING_LOG_LEVEL': '{{ ti.xcom_pull(key="ACCUTUNING_LOG_LEVEL")}}',
              'ACCUTUNING_WORKSPACE': '{{ ti.xcom_pull(key="ACCUTUNING_WORKER_WORKSPACE") }}',
              },
    get_logs=True,
    dag=dag,
)

worker_success = KubernetesPodExPostOperator(
    namespace='{{ ti.xcom_pull(key="ACCUTUNING_K8S_WORKER_NAMESPACE")}}',
    name="worker_success",
    task_id="worker_success",
    env_vars=make_env_var(),
    cmds=["python3"],
    get_logs=True,
    dag=dag,
    trigger_rule='all_success',
)

worker_fail = KubernetesPodExPostOperator(
    namespace='{{ ti.xcom_pull(key="ACCUTUNING_K8S_WORKER_NAMESPACE")}}',
    name="worker_fail",
    task_id="worker_fail",
    env_vars=make_env_var(),
    cmds=["python3"],
    get_logs=True,
    dag=dag,
    trigger_rule='one_failed',
)


# one_success로 해야 skip된 task를 무시함
end = DummyOperator(
    task_id='end',
    trigger_rule='one_success',
    dag=dag,
)

timer = PythonOperator(task_id='timer', provide_context=True, python_callable=_check, dag=dag)

start >> parameters >> before_worker >> worker_env >> worker

worker >> worker_success >> end
worker >> worker_fail >> end

start >> timer >> end
