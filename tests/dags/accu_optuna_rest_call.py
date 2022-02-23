from airflow import DAG
from datetime import datetime, timedelta
from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from kubernetes.client import models as k8s # you should write this sentence when you could use volume, etc 
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.edgemodifier import Label #label 쓰기 위한 library

from airflow.models import TaskInstance

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
    'accutuning_optuna_rest_call', default_args=default_args, schedule_interval=timedelta(minutes=10))

start = DummyOperator(task_id='start', dag=dag)


volume_mount = k8s.V1VolumeMount(
    name='test-volume', mount_path='/workspace', sub_path=None, read_only=False
)
volume = k8s.V1Volume(
    name='test-volume',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='test-volume', read_only=False),
)
configmaps = [
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='airflow-test-1')),
]  
init_container_volume_mounts = [
    k8s.V1VolumeMount(mount_path='/workspace', name='test-volume', read_only=False)
]


# init_container = k8s.V1Container(
#     name="init-container",
#     image="pooh97/accu-app:latest",    
#     volume_mounts=init_container_volume_mounts,
# )
   
# python3 /code/manage.py ml_parse_pre --experiment=19 --uuid='4043104546ca4c0597ba5341607ba06f' --timeout=200
# python3 /code/manage.py ml_parse_pre --experiment=19 --uuid=$ACCUTUNING_UUID --timeout=$ACCUTUNING_TIMEOUT
# env
# python3 /code/manage.py ml_parse_pre --experiment=$ACCUTUNING_EXPERIMENT_ID --uuid=$ACCUTUNING_UUID --timeout=$ACCUTUNING_TIMEOUT
    
optuna_pre = KubernetesPodOperator(
    namespace='default',
    image="pooh97/accu-app:latest",    
    volumes=[volume],
    volume_mounts=[volume_mount],
    name="optuna_pre",
    task_id="optuna_pre",
#     init_containers=[init_container],
    #env_vars={'ACCUTUNING_LOG_LEVEL': '{{dag_run.conf["ACCUTUNING_LOG_LEVEL"] if dag_run else "" }}', 'ACCUTUNING_WORKSPACE':'{{dag_run.conf["ACCUTUNING_WORKSPACE"] if dag_run else "" }}'},
    env_vars={
              'ACCUTUNING_WORKSPACE':'/workspace/',
              'ACCUTUNING_LOG_LEVEL':'INFO',
              'ACCUTUNING_USE_LABELER':'1',
              'ACCUTUNING_USE_CLUSTERING':'1',
              'DJANGO_SETTINGS_MODULE':'accutuning.settings'      
             },
#     cmds=["bash", "-cx"],
#     arguments=["python3 /code/manage.py ml_parse_pre --experiment=19 --uuid='4043104546ca4c0597ba5341607ba06f' --timeout=200"],
    cmds=["bash", "-cx", "python3"],
    arguments=["/code/manage.py", "{{dag_run.conf['ACCUTUNING_COMMAND'] if dag_run else "" }}", "--experiment={{dag_run.conf['ACCUTUNING_EXPERIMENT_ID'] if dag_run else "" }}",  "--uuid='{{dag_run.conf['ACCUTUNING_UUID'] if dag_run else "" }}'", "--timeout={{dag_run.conf['TIMEOUT'] if dag_run else "" }}"],     
#     arguments=["/code/manage.py", "{{dag_run.conf["ACCUTUNING_COMMAND"] if dag_run else "" }}", "--experiment={{dag_run.conf["ACCUTUNING_EXPERIMENT_ID"] if dag_run else "" }}",  "--uuid='{{dag_run.conf["ACCUTUNING_UUID"] if dag_run else "" }}'", "--timeout=200"],   
    
    get_logs=True,
    dag=dag,    
)

optuna_main = KubernetesPodOperator(
    namespace='default',
#     image="pooh97/accu-worker:latest",    
    image="{{dag_run.conf['ACCUTUNING_WORKER_IMAGE']}}",    
    volumes=[volume],
    volume_mounts=[volume_mount],
    name="optuna_main",
    task_id="optuna_main",
    env_vars={'ACCUTUNING_LOG_LEVEL': '{{dag_run.conf["ACCUTUNING_LOG_LEVEL"] if dag_run else "" }}', 'ACCUTUNING_WORKSPACE':'{{dag_run.conf["ACCUTUNING_WORKER_WORKSPACE"] if dag_run else "" }}'},
#     env_vars={'ACCUTUNING_LOG_LEVEL': 'INFO', 'ACCUTUNING_WORKSPACE':'/workspace/experiment_0019/experimentprocess_0050'},
    get_logs=True,
    dag=dag,    
)


# python3 /code/manage.py ml_parse_pre --experiment=19 --uuid='4043104546ca4c0597ba5341607ba06f' --timeout=200
# python3 /code/manage.py ml_parse_pre --experiment=19 --uuid=$ACCUTUNING_UUID --timeout=$ACCUTUNING_TIMEOUT
# env
# python3 /code/manage.py ml_parse_post --experiment=$ACCUTUNING_EXPERIMENT_ID --uuid=$ACCUTUNING_UUID --timeout=$ACCUTUNING_TIMEOUT
# ml_parse_post = KubernetesPodOperator(
#     namespace='default',
#     image="pooh97/accu-app:latest",    
#     volumes=[volume],
#     volume_mounts=[volume_mount],
#     name="ml_parse_post",
#     task_id="ml_parse_post",
# #     init_containers=[init_container],
#     #env_vars={'ACCUTUNING_LOG_LEVEL': '{{dag_run.conf["ACCUTUNING_LOG_LEVEL"] if dag_run else "" }}', 'ACCUTUNING_WORKSPACE':'{{dag_run.conf["ACCUTUNING_WORKSPACE"] if dag_run else "" }}'},
#     env_vars={
#               'ACCUTUNING_WORKSPACE':'/workspace/',
#               'ACCUTUNING_LOG_LEVEL':'INFO',
#               'ACCUTUNING_USE_LABELER':'1',
#               'ACCUTUNING_USE_CLUSTERING':'1',
#               'DJANGO_SETTINGS_MODULE':'accutuning.settings'      

#              },
# #     cmds=["bash", "-cx"],
#     cmds=["bash", "-cx", "python3"],
#     arguments=["/code/manage.py", "{{dag_run.conf["ACCUTUNING_COMMAND"] if dag_run else "" }}", "--experiment={{dag_run.conf["ACCUTUNING_EXPERIMENT_ID"] if dag_run else "" }}",  "--uuid='{{dag_run.conf["ACCUTUNING_UUID"] if dag_run else "" }}'", "--timeout={{dag_run.conf["TIMEOUT"] if dag_run else "" }}"],   
# #     arguments=["/code/manage.py", "ml_parse_post", "--experiment=19",  "--uuid='4043104546ca4c0597ba5341607ba06f'", "--timeout=200"],   
#     get_logs=True,
#     dag=dag,        
#     trigger_rule='all_success',
# )


failure = DummyOperator(
    task_id='failure',
    trigger_rule='one_failed',    
    dag=dag,
)

## one_success로 해야 skip된 task를 무시함
end = DummyOperator(
    task_id='end',
    trigger_rule='one_success',
    dag=dag,
)

options = ['ml_parse_post', 'failure']

# def which_path():
#   '''
#   return the task_id which to be executed
#   '''
#   if True:
#     task_id = 'ml_parse_post'
#   else:
#     task_id = 'failure'
#   return task_id

#  dag_instance = kwargs['dag']
#  operator_instance = dag_instance.get_task("task_id")
#  task_status = TaskInstance(operator_instance, execution_date).current_state()
    

    
# check_situation = BranchPythonOperator(
#     task_id='check_situation',
#     python_callable=task_state,
#     dag=dag,
#     )

# start >> Label("app 중 ml_parse_pre Call") >> optuna_pre >> Label("common_module worker 중 Call") >> optuna_main >> end
start >> Label("common_module worker 중 Call") >> optuna_main >> end

# ml_parse_main >> Label("worker 작업 성공시(app 중 ml_parse_post Call)") >> ml_parse_post >> end
# ml_parse_main >> Label("worker 작업 실패시") >> failure >> end

# start >> ml_parse_pre >> ml_parse_main >> check_situation
# check_situation >> ml_parse_post >> success >> finish 
# check_situation >> failure >> send_error >> finish  
