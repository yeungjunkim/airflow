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
    'ml_run_k8s', default_args=default_args, schedule_interval=timedelta(minutes=10))

start = DummyOperator(task_id='start', dag=dag)


volume_mount = k8s.V1VolumeMount(
    name='test-volume', mount_path='/workspace', sub_path=None, read_only=False
)

volume = k8s.V1Volume(
    name='test-volume',
#     persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='test-volume', read_only=False),
    host_path=k8s.V1HostPathVolumeSource(path='/workspace'),
)

configmaps = [
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='airflow-test-1')),
]  
   
# python3 /code/manage.py ml_parse_pre --experiment=19 --uuid='4043104546ca4c0597ba5341607ba06f' --timeout=200
# python3 /code/manage.py ml_parse_pre --experiment=19 --uuid=$ACCUTUNING_UUID --timeout=$ACCUTUNING_TIMEOUT
# env
# python3 /code/manage.py ml_parse_pre --experiment=$ACCUTUNING_EXPERIMENT_ID --uuid=$ACCUTUNING_UUID --timeout=$ACCUTUNING_TIMEOUT
    
ml_run_pre = KubernetesPodOperator(
    namespace='default',
    image='{{dag_run.conf.ACCUTUNING_APP_IMAGE}}',    
    # image='pooh97/accu-app:latest',    
    volumes=[volume],
    volume_mounts=[volume_mount],
    name="ml_run_before",
    task_id="ml_run_before",
    env_vars={
               'ACCUTUNING_WORKSPACE':'{{dag_run.conf["ACCUTUNING_WORKSPACE"]}}',
               'ACCUTUNING_LOG_LEVEL':'{{dag_run.conf["ACCUTUNING_LOG_LEVEL"]}}',
               'ACCUTUNING_USE_LABELER':'{{dag_run.conf["ACCUTUNING_USE_LABELER"]}}',
               'ACCUTUNING_USE_CLUSTERING':'{{dag_run.conf["ACCUTUNING_USE_CLUSTERING"]}}',
               'DJANGO_SETTINGS_MODULE':'{{dag_run.conf["DJANGO_SETTINGS_MODULE"]}}'     
    },
#     env_vars='{{dag_run.conf.worker_env_vars}}',
    cmds=["python3"],
    arguments=["/code/manage.py", "{{dag_run.conf['ACCUTUNING_DJANGO_COMMAND']}}", "--experiment={{dag_run.conf['ACCUTUNING_EXPERIMENT_ID']}}",  "--uuid={{dag_run.conf['ACCUTUNING_UUID']}}", "--timeout={{dag_run.conf['ACCUTUNING_TIMEOUT']}}","--execute_range=before"],  
#     arguments="[{{dag_run.conf.before_command}}]",   
#     cmds=['{{dag_run.conf.before_command1}}'],
#     arguments=['{{dag_run.conf.before_command2}}'],   
    get_logs=True,
    dag=dag,    
)

ml_run_main = KubernetesPodOperator(
    namespace='default',
    image="{{dag_run.conf.ACCUTUNING_WORKER_IMAGE}}",    
    volumes=[volume],
    volume_mounts=[volume_mount],
    name="ml_run_main",
    task_id="ml_run_main",
    env_vars={'ACCUTUNING_LOG_LEVEL': '{{dag_run.conf["ACCUTUNING_LOG_LEVEL"]}}', 'ACCUTUNING_WORKSPACE':'{{dag_run.conf["ACCUTUNING_WORKER_WORKSPACE"]}}'},
    get_logs=True,
    dag=dag,    
)


# python3 /code/manage.py ml_parse_pre --experiment=19 --uuid='4043104546ca4c0597ba5341607ba06f' --timeout=200
# python3 /code/manage.py ml_parse_pre --experiment=19 --uuid=$ACCUTUNING_UUID --timeout=$ACCUTUNING_TIMEOUT
# env
# python3 /code/manage.py ml_parse_post --experiment=$ACCUTUNING_EXPERIMENT_ID --uuid=$ACCUTUNING_UUID --timeout=$ACCUTUNING_TIMEOUT
ml_run_success = KubernetesPodOperator(
    namespace='default',
    image='{{dag_run.conf["ACCUTUNING_APP_IMAGE"]}}',        
    volumes=[volume],
    volume_mounts=[volume_mount],
    name="ml_run_success",
    task_id="ml_run_success",
    env_vars={
              'ACCUTUNING_WORKSPACE':'{{dag_run.conf["ACCUTUNING_WORKSPACE"]}}',
              'ACCUTUNING_LOG_LEVEL':'{{dag_run.conf["ACCUTUNING_LOG_LEVEL"]}}',
              'ACCUTUNING_USE_LABELER':'{{dag_run.conf["ACCUTUNING_USE_LABELER"]}}',
              'ACCUTUNING_USE_CLUSTERING':'{{dag_run.conf["ACCUTUNING_USE_CLUSTERING"]}}',
              'DJANGO_SETTINGS_MODULE':'{{dag_run.conf["DJANGO_SETTINGS_MODULE"]}}'     
             },
#     env_vars='{{dag_run.conf.worker_env_vars}}',   
    # cmds=["python3"],
    # arguments=["/code/manage.py", "ml_parse_post", "--experiment={{dag_run.conf['ACCUTUNING_EXPERIMENT_ID']}}",  "--uuid={{dag_run.conf['ACCUTUNING_UUID']}}", "--timeout={{dag_run.conf['ACCUTUNING_TIMEOUT']}}"],   
    cmds=["python3"],
    arguments=["/code/manage.py", "ml_parse", "--experiment={{dag_run.conf['ACCUTUNING_EXPERIMENT_ID']}}",  "--uuid={{dag_run.conf['ACCUTUNING_UUID']}}", "--timeout={{dag_run.conf['ACCUTUNING_TIMEOUT']}}","--execute_range=after"],   
#     arguments="{{dag_run.conf.after_command}}",       
#     arguments=["/code/manage.py", "ml_parse", "--experiment={{dag_run.conf['ACCUTUNING_EXPERIMENT_ID']}}",  "--uuid={{dag_run.conf['ACCUTUNING_UUID']}}", "--timeout={{dag_run.conf['ACCUTUNING_TIMEOUT']}}","--execute_range=after"],   
    
#     arguments=["/code/manage.py", "ml_parse", "--experiment={{dag_run.conf['ACCUTUNING_EXPERIMENT_ID']}}",  "--uuid={{dag_run.conf['ACCUTUNING_UUID']}}", "--timeout={{dag_run.conf['ACCUTUNING_TIMEOUT']}}","--execute_range=after"],   
#     cmds=['{{dag_run.conf.after_command1}}'],
#     arguments=['{{dag_run.conf.after_command2}}'],       
    get_logs=True,
    dag=dag,        
    trigger_rule='all_success',
)

ml_run_fail = KubernetesPodOperator(
    namespace='default',
    image='{{dag_run.conf.ACCUTUNING_APP_IMAGE}}',        
    volumes=[volume],
    volume_mounts=[volume_mount],
    name="ml_run_fail",
    task_id="ml_run_fail",
    #env_vars={'ACCUTUNING_LOG_LEVEL': '{{dag_run.conf["ACCUTUNING_LOG_LEVEL"] if dag_run else "" }}', 'ACCUTUNING_WORKSPACE':'{{dag_run.conf["ACCUTUNING_WORKSPACE"] if dag_run else "" }}'},
    env_vars={
              'ACCUTUNING_WORKSPACE':'{{dag_run.conf["ACCUTUNING_WORKSPACE"]}}',
              'ACCUTUNING_LOG_LEVEL':'{{dag_run.conf["ACCUTUNING_LOG_LEVEL"]}}',
              'ACCUTUNING_USE_LABELER':'{{dag_run.conf["ACCUTUNING_USE_LABELER"]}}',
              'ACCUTUNING_USE_CLUSTERING':'{{dag_run.conf["ACCUTUNING_USE_CLUSTERING"]}}',
              'DJANGO_SETTINGS_MODULE':'{{dag_run.conf["DJANGO_SETTINGS_MODULE"]}}'        
             },
#     env_vars='{{dag_run.conf.worker_env_vars}}',   
#     cmds=["python"],
#     arguments=["/code/manage.py", "ml_parse", "--experiment={{dag_run.conf['ACCUTUNING_EXPERIMENT_ID']}}",  "--uuid={{dag_run.conf['ACCUTUNING_UUID']}}", "--timeout={{dag_run.conf['ACCUTUNING_TIMEOUT']}}","--execute_range=after"],   
    cmds=["python3"],
    arguments=["/code/manage.py", "ml_parse", "--experiment={{dag_run.conf['ACCUTUNING_EXPERIMENT_ID']}}",  "--uuid={{dag_run.conf['ACCUTUNING_UUID']}}", "--timeout={{dag_run.conf['ACCUTUNING_TIMEOUT']}}","--execute_range=after"],   
    
#     arguments="{{dag_run.conf.after_command}}",       

#     arguments=["/code/manage.py", "ml_parse", "--experiment={{dag_run.conf['ACCUTUNING_EXPERIMENT_ID']}}",  "--uuid={{dag_run.conf['ACCUTUNING_UUID']}}", "--timeout={{dag_run.conf['ACCUTUNING_TIMEOUT']}}"],   
#     cmds=['{{dag_run.conf.after_command1}}'],
#     arguments=['{{dag_run.conf.after_command2}}'],  
    get_logs=True,
    dag=dag,        
    trigger_rule='one_failed',
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

start >> Label("app 중 ml_parse_pre Call") >> ml_run_pre >> Label("common_module worker 중 Call") >> ml_run_main 

ml_run_main >> Label("worker 작업 성공시(app 중 ml_parse_success Call)") >> ml_run_success >> end
ml_run_main >> Label("worker 작업 실패시(app 중 ml_parse_fail Call)") >> ml_run_fail >> end

# start >> ml_parse_pre >> ml_parse_main >> check_situation
# check_situation >> ml_parse_post >> success >> finish 
# check_situation >> failure >> send_error >> finish  
