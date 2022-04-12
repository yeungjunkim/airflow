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
# import json
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
    'ml_run_k8s', default_args=default_args, schedule_interval=timedelta(minutes=10))

start = DummyOperator(task_id='start', dag=dag)

volume_mount = k8s.V1VolumeMount(
    name='test-volume', mount_path='/workspace', sub_path=None, read_only=False
)

volume = k8s.V1Volume(
    name='test-volume',
    # persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='test-volume', read_only=False),
    host_path=k8s.V1HostPathVolumeSource(path='/workspace'),
)

configmaps = [
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='airflow-test-1')),
]


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
        'None': 'None',
    }
    return command_dict[experiment_process_type]


# def get_next_experiment_process_type(experiment_process_type, use_ensemble):
#     command_list = [
#         'parse', 'preprocess', 'optuna', 'ensemble', 'deploy', 'predict', 'labeling', 'lb_predict', 'modelstat', 'dataset_eda',
#     ]
#     print("experiment_process_type = {}".format(experiment_process_type))
#     print("use_ensemble = {}".format(use_ensemble))
#     print("experiment_process_type == 'optuna' = {}".format(experiment_process_type == 'optuna'))
#     print("use_ensemble == 'False' = {}".format(use_ensemble == 'False'))

#     if experiment_process_type == "preprocess" or \
#        experiment_process_type == "optuna" or \
#        experiment_process_type == "ensemble":
#         if experiment_process_type == 'optuna' and use_ensemble == 'False':  # to deploy
#             return command_list[command_list.index(experiment_process_type) + 2]
#         else:
#             return command_list[command_list.index(experiment_process_type) + 1]
#     else:
#         return 'None'


def make_uuid():
    import uuid
    return str(uuid.uuid4()).replace('-', '')


def make_accutuning_docker_command(django_command, experiment_id, container_uuid, execute_range, experiment_process_type, proceed_next, targets):
    command = f'''/code/manage.py {django_command}
--experiment={experiment_id} --uuid={container_uuid} --execute_range={execute_range}
--experiment_process_type={experiment_process_type} --proceed_next={proceed_next} '''
    return (command + '\n'.join([f'--{k}={v}' for (k, v) in targets.items() if v])).split()


def make_parameters(**kwargs):
    experiment_id = kwargs['dag_run'].conf['ACCUTUNING_EXPERIMENT_ID']
    experiment_process_type = kwargs['dag_run'].conf['experiment_process_type']
    # experiment_target = kwargs['dag_run'].conf['experiment_target']
    proceed_next = kwargs['dag_run'].conf['proceed_next']
    use_ensemble = kwargs['dag_run'].conf['use_ensemble']

    print("experiment_id = {}".format(experiment_id))
    print("experiment_process_type = {}".format(experiment_process_type))
    print("use_ensemble = {}".format(use_ensemble))
    container_uuid = make_uuid()
    django_command = get_command_name(experiment_process_type)
    # kwargs['dag_run'].conf['ACCUTUNING_UUID'] = container_uuid
    # kwargs['dag_run'].conf['ACCUTUNING_DJANGO_COMMAND'] = django_command

    targets = dict(
        target_dataset=kwargs['dag_run'].conf.get('target_dataset'),
        target_dataset_eda=kwargs['dag_run'].conf.get('target_dataset_eda'),
        target_prediction=kwargs['dag_run'].conf.get('target_prediction'),
        target_model_base=kwargs['dag_run'].conf.get('target_model_base'),
        target_deployment=kwargs['dag_run'].conf.get('target_deployment'),
        target_source=kwargs['dag_run'].conf.get('target_source'),
    )

    kwargs['task_instance'].xcom_push(key='ACCUTUNING_UUID', value=container_uuid)
    kwargs['task_instance'].xcom_push(key='ACCUTUNING_DJANGO_COMMAND', value=django_command)

    # print("kwargs['dag_run'].conf['ACCUTUNING_UUID'] = {}".format(kwargs['dag_run'].conf['ACCUTUNING_UUID']))
    # print("kwargs['dag_run'].conf['ACCUTUNING_DJANGO_COMMAND'] = {}".format(kwargs['dag_run'].conf['ACCUTUNING_DJANGO_COMMAND']))

    django_command = get_command_name(experiment_process_type)
    # django_next_process = get_next_experiment_process_type(experiment_process_type, use_ensemble)
    docker_command_before = make_accutuning_docker_command(django_command, experiment_id, container_uuid, 'before', experiment_process_type, proceed_next, targets)
    docker_command_after = make_accutuning_docker_command(django_command, experiment_id, container_uuid, 'after', experiment_process_type, proceed_next, targets)

    # print("django_next_command = {}".format(django_next_command))
    # print("django_next_process = {}".format(django_next_process))

    # kwargs['task_instance'].xcom_push(key='NEXT_ACCUTUNING_UUID', value=container_uuid)
    # kwargs['task_instance'].xcom_push(key='NEXT_ACCUTUNING_DJANGO_COMMAND', value=django_next_command)
    # kwargs['task_instance'].xcom_push(key='NEXT_ACCUTUNING_DJANGO_PROCESS', value=django_next_process)
    kwargs['task_instance'].xcom_push(key='before_command', value=docker_command_before)
    kwargs['task_instance'].xcom_push(key='after_command', value=docker_command_after)


def make_worker_env(**kwargs):
    workspace_path = kwargs['task_instance'].xcom_pull(task_ids='before_worker', key='return_value')["worker_workspace"]

    # worker_env_vars_str = kwargs['dag_run'].conf['worker_env_vars']

    print(f'workspace_path:{workspace_path}')
    # print(f'worker_env_vars:{worker_env_vars_str}')

    # env_dict = json.loads(worker_env_vars_str)
    # env_dict['ACCUTUNING_WORKSPACE'] = workspace_path
    kwargs['task_instance'].xcom_push(key='ACCUTUNING_WORKER_WORKSPACE', value=workspace_path)

    # worker_env_vars = json.dumps(env_dict)

    # print(f'worker_env_vars:{worker_env_vars}')

    # kwargs['task_instance'].xcom_push(key='worker_env_vars', value=worker_env_vars)


parameters = PythonOperator(task_id='make_parameters', python_callable=make_parameters, dag=dag)
 
before_worker = KubernetesPodOperator(
    namespace='default',
    image='{{dag_run.conf.ACCUTUNING_APP_IMAGE}}',
    # image='pooh97/accu-app:latest',
    volumes=[volume],
    volume_mounts=[volume_mount],
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
    arguments=
        "{{ ti.xcom_pull(key='before_command') }}",

        # "/code/manage.py",
        # "{{ ti.xcom_pull(key='ACCUTUNING_DJANGO_COMMAND') }}",
        # "--experiment={{dag_run.conf.ACCUTUNING_EXPERIMENT_ID']}}",
        # "--uuid={{ ti.xcom_pull(key='ACCUTUNING_UUID') }}",
        # "--timeout={{dag_run.conf.ACCUTUNING_TIMEOUT']}}",
        # "--execute_range=before",
        # "--experiment_process_type={{dag_run.conf.experiment_process_type']}}",
        # # "--experiment_target={{dag_run.conf.experiment_target']}}",
        # "--proceed_next={{dag_run.conf.proceed_next}}",
        # "--target_dataset={{dag_run.conf.target_dataset}}",
        # "--target_dataset_eda={{dag_run.conf.target_dataset_eda']}}",
        # "--target_prediction={{dag_run.conf.target_prediction']}}",
        # "--target_model_base={{dag_run.conf.target_model_base']}}",
        # "--target_deployment={{dag_run.conf.target_deployment']}}",
        # "--target_source={{dag_run.conf.target_source']}}",
    # ,
    do_xcom_push=True,
    image_pull_policy='Always',
    get_logs=True,
    dag=dag,
)


worker_env = PythonOperator(task_id='make_worker_env', python_callable=make_worker_env, dag=dag)

worker = KubernetesPodOperator(
    namespace='default',
    image="{{dag_run.conf.ACCUTUNING_WORKER_IMAGE}}",
    volumes=[volume],
    volume_mounts=[volume_mount],
    name="worker",
    task_id="worker",
    env_vars={'ACCUTUNING_LOG_LEVEL': '{{dag_run.conf.ACCUTUNING_LOG_LEVEL}}', 'ACCUTUNING_WORKSPACE': '{{ ti.xcom_pull(key="ACCUTUNING_WORKER_WORKSPACE") }}'},
    image_pull_policy='Always',
    get_logs=True,
    dag=dag,
)

worker_success = KubernetesPodOperator(
    namespace='default',
    image='{{dag_run.conf.ACCUTUNING_APP_IMAGE}}',
    volumes=[volume],
    volume_mounts=[volume_mount],
    name="worker_success",
    task_id="worker_success",
    env_vars={
        'ACCUTUNING_WORKSPACE': '{{dag_run.conf.ACCUTUNING_WORKSPACE}}',
        'ACCUTUNING_LOG_LEVEL': '{{dag_run.conf.ACCUTUNING_LOG_LEVEL}}',
        'ACCUTUNING_USE_LABELER': '{{dag_run.conf.ACCUTUNING_USE_LABELER}}',
        'ACCUTUNING_USE_CLUSTERING': '{{dag_run.conf.ACCUTUNING_USE_CLUSTERING}}',
        'DJANGO_SETTINGS_MODULE': '{{dag_run.conf.DJANGO_SETTINGS_MODULE}}'
    },
    # env_vars='{{dag_run.conf.worker_env_vars}}',
    # cmds=["python3"],
    # arguments=["/code/manage.py", ""{{dag_run.conf.ACCUTUNING_DJANGO_COMMAND']}}"", "--experiment={{dag_run.conf.ACCUTUNING_EXPERIMENT_ID']}}",  "--uuid={{dag_run.conf.ACCUTUNING_UUID']}}", "--timeout={{dag_run.conf.ACCUTUNING_TIMEOUT']}}"],
    cmds=["python3"],
    arguments=
        # "/code/manage.py",
        # "{{ ti.xcom_pull(key='ACCUTUNING_DJANGO_COMMAND') }}",
        # "--experiment={{dag_run.conf.ACCUTUNING_EXPERIMENT_ID']}}",
        # "--uuid={{ ti.xcom_pull(key='ACCUTUNING_UUID') }}",
        # "--timeout={{dag_run.conf.ACCUTUNING_TIMEOUT']}}",
        # "--execute_range=after",
        # "--experiment_process_type={{dag_run.conf.experiment_process_type']}}",
        # # "--experiment_target={{dag_run.conf.experiment_target']}}",
        # "--proceed_next={{dag_run.conf.proceed_next']}}",
        # "--target_dataset={{dag_run.conf.target_dataset'] if dag_run.conf.target_dataset']}}",
        # # "--target_dataset_eda={{dag_run.conf.target_dataset_eda'] if dag_run.conf.target_dataset_eda'] }}",
        # # "--target_prediction={{dag_run.conf.target_prediction'] if dag_run.conf.target_prediction']}}",
        # # "--target_model_base={{dag_run.conf.target_model_base'] if dag_run.conf.target_model_base']}}",
        # # "--target_deployment={{dag_run.conf.target_deployment'] if dag_run.conf.target_deployment']}}",
        # # "--target_source={{dag_run.conf.target_source'] if dag_run.conf.target_source']}}",
        "{{ ti.xcom_pull(key='after_command') }}",
    # ,
    image_pull_policy='Always',
    get_logs=True,
    dag=dag,
    trigger_rule='all_success',
)

worker_fail = KubernetesPodOperator(
    namespace='default',
    image='{{dag_run.conf.ACCUTUNING_APP_IMAGE}}',
    volumes=[volume],
    volume_mounts=[volume_mount],
    name="worker_fail",
    task_id="worker_fail",
    # env_vars={'ACCUTUNING_LOG_LEVEL': '{{dag_run.conf.ACCUTUNING_LOG_LEVEL"] if dag_run else "" }}', 'ACCUTUNING_WORKSPACE':'{{dag_run.conf.ACCUTUNING_WORKSPACE"] if dag_run else "" }}'},
    env_vars={
        'ACCUTUNING_WORKSPACE': '{{dag_run.conf.ACCUTUNING_WORKSPACE}}',
        'ACCUTUNING_LOG_LEVEL': '{{dag_run.conf.ACCUTUNING_LOG_LEVEL}}',
        'ACCUTUNING_USE_LABELER': '{{dag_run.conf.ACCUTUNING_USE_LABELER}}',
        'ACCUTUNING_USE_CLUSTERING': '{{dag_run.conf.ACCUTUNING_USE_CLUSTERING}}',
        'DJANGO_SETTINGS_MODULE': '{{dag_run.conf.DJANGO_SETTINGS_MODULE}}'
    },
    # env_vars='{{dag_run.conf.worker_env_vars}}',
    # cmds=["python"],
    # arguments=["/code/manage.py", "ml_parse", "--experiment={{dag_run.conf.ACCUTUNING_EXPERIMENT_ID']}}",  "--uuid={{dag_run.conf.ACCUTUNING_UUID']}}", "--timeout={{dag_run.conf.ACCUTUNING_TIMEOUT']}}","--execute_range=after"],
    cmds=["python3"],
    arguments=
        # "/code/manage.py",
        # "{{ ti.xcom_pull(key='ACCUTUNING_DJANGO_COMMAND') }}",
        # "--experiment={{dag_run.conf.ACCUTUNING_EXPERIMENT_ID']}}",
        # "--uuid={{ ti.xcom_pull(key='ACCUTUNING_UUID') }}",
        # "--timeout={{dag_run.conf.ACCUTUNING_TIMEOUT']}}",
        # "--execute_range=after",
        # "--experiment_process_type={{dag_run.conf.experiment_process_type']}}",
        # # "--experiment_target={{dag_run.conf.experiment_target']}}",
        # "--proceed_next={{dag_run.conf.proceed_next']}}",
        # "--target_dataset={{dag_run.conf.target_dataset'] if dag_run.conf.target_dataset']}}",
        # # "--target_dataset_eda={{dag_run.conf.target_dataset_eda'] if dag_run.conf.target_dataset_eda'] }}",
        # # "--target_prediction={{dag_run.conf.target_prediction'] if dag_run.conf.target_prediction']}}",
        # # "--target_model_base={{dag_run.conf.target_model_base'] if dag_run.conf.target_model_base']}}",
        # # "--target_deployment={{dag_run.conf.target_deployment'] if dag_run.conf.target_deployment']}}",
        # # "--target_source={{dag_run.conf.target_source'] if dag_run.conf.target_source']}}",
        "{{ ti.xcom_pull(key='after_command') }}",
    # ,
    image_pull_policy='Always',
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


# trigger = TriggerDagRunOperator(task_id='trigger_dagrun',
#                                 # trigger_dag_id="ml_run_k8s",
#                                 trigger_dag_id="ml_automl",
#                                 # python_callable=conditionally_trigger,
#                                 conf={'condition_param': True,
#                                         'ACCUTUNING_WORKSPACE':'{{dag_run.conf.ACCUTUNING_WORKSPACE}}',
#                                         'ACCUTUNING_LOG_LEVEL':'{{dag_run.conf.ACCUTUNING_LOG_LEVEL}}',
#                                         'ACCUTUNING_USE_LABELER':'{{dag_run.conf.ACCUTUNING_USE_LABELER}}',
#                                         'ACCUTUNING_USE_CLUSTERING':'{{dag_run.conf.ACCUTUNING_USE_CLUSTERING}}',
#                                         'DJANGO_SETTINGS_MODULE':'{{dag_run.conf.DJANGO_SETTINGS_MODULE}}',
#                                         'ACCUTUNING_DJANGO_COMMAND':"{{ ti.xcom_pull(key=\"NEXT_ACCUTUNING_DJANGO_COMMAND\") }}",
#                                         'ACCUTUNING_EXPERIMENT_ID':'{{dag_run.conf.ACCUTUNING_EXPERIMENT_ID}}',
#                                         'ACCUTUNING_UUID':"{{ ti.xcom_pull(key=\"NEXT_ACCUTUNING_UUID\") }}",
#                                         'ACCUTUNING_TIMEOUT':'{{dag_run.conf.ACCUTUNING_TIMEOUT}}',
#                                         'ACCUTUNING_APP_IMAGE':'{{dag_run.conf.ACCUTUNING_APP_IMAGE}}',
#                                         'ACCUTUNING_WORKER_IMAGE':'{{dag_run.conf.ACCUTUNING_WORKER_IMAGE}}',
#                                         'ACCUTUNING_WORKER_WORKSPACE':'{{ti.xcom_pull(key=\'ACCUTUNING_WORKER_WORKSPACE\')}}',
#                                         'experiment_id':'{{dag_run.conf.experiment_id}}',
#                                         'experiment_process_type':'{{ti.xcom_pull(key=\"NEXT_ACCUTUNING_DJANGO_PROCESS\")}}',
#                                         'experiment_target':'{{dag_run.conf.experiment_target}}',
#                                         'proceed_next':'{{dag_run.conf.proceed_next}}',
#                                         'use_ensemble':'{{dag_run.conf.use_ensemble}}',
#                                         },
#                                 trigger_rule='one_success',
#                                 dag=dag)


# branch_end = DummyOperator(task_id='branch_end', dag=dag)

# def chk_ml_parse(**kwargs):
#     django_command = kwargs['task_instance'].xcom_pull(key='ACCUTUNING_DJANGO_COMMAND')
#     print("chk_ml_parse django_command = {}".format(django_command))

#     if django_command=="ml_preprocess" or django_command=="ml_optuna" or django_command=="ml_ensemble":
#         return 'trigger_dagrun'
#     else:
#         return 'branch_end'

# branch_task = BranchPythonOperator(
#     task_id='branching',
#     python_callable=chk_ml_parse,
#     dag=dag,
# )


start >> Label("parameter") >> parameters >> Label("app 중 before_worker Call") >> before_worker >> Label("common_module worker 중 Call") >> worker_env >> worker

# worker >> Label("worker 작업 성공시(app 중 worker_success Call)") >> worker_success >> end >> branch_task
worker >> Label("worker 작업 성공시(app 중 worker_success Call)") >> worker_success >> end
worker >> Label("worker 작업 실패시(app 중 worker_fail Call)") >> worker_fail >> end

# branch_task >> trigger
# branch_task >> branch_end

# start >> ml_parse_pre >> ml_parse_main >> check_situation
# check_situation >> ml_parse_post >> success >> finish
# check_situation >> failure >> send_error >> finish
