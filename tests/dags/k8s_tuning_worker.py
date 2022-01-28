from airflow import DAG
from datetime import datetime, timedelta
from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from kubernetes.client import models as k8s # you should write this sentence when you could use volume, etc 

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
    'accutuning-worker-test', default_args=default_args, schedule_interval=timedelta(minutes=10))


start = DummyOperator(task_id='start', dag=dag)

# cmd = 'eval $(/usr/local/bin/minikube -p /usr/local/bin/minikube docker-env)'
cmd = 'pwd;ls -al;uname -a'
setting = BashOperator(task_id='setting', bash_command=cmd, dag=dag)

#  docker run -it 
#     -e ACCUTUNING_LOG_LEVEL=INFO 
#     -e ACCUTUNING_WORKSPACE=/workspace/experiment_0008/experimentprocess_0037 
#     -v /Users/yeongjunkim/dev/accutuning_gitlab/accutuning/.workspace:/workspace 
#         accutuning/modeler-common:latest

# secret_file = Secret('volume', '/etc/sql_conn', 'airflow-secrets', 'sql_alchemy_conn')
# secret_file = Secret('volume', '/etc/sql_conn', 'sql_alchemy_conn')
# secret_env = Secret('env', 'SQL_CONN', 'airflow-secrets', 'sql_alchemy_conn')
# secret_all_keys = Secret('env', None, 'airflow-secrets-2')
secret_all_keys = Secret('env', None, 'default-token-8cv8w')



volume_mount = k8s.V1VolumeMount(
    name='test-volume', mount_path='/workspace', sub_path=None, read_only=False
)

volume = k8s.V1Volume(
    name='test-volume',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='test-volume', read_only=False),
#     host_path=k8s.V1HostPathVolumeSource(path='/workspace'),
)

# configmaps = [
#     k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='test-configmap-1')),
#     k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='test-configmap-2')),
# ]

port = k8s.V1ContainerPort(name='http', container_port=80)

# init_container_volume_mounts = [
#     k8s.V1VolumeMount(mount_path='/workspace', name='test-volume', sub_path=None, read_only=True)
# ]

# init_container_volume_mounts = [
#     k8s.V1VolumeMount(mount_path='/workspace', name='test-volume', sub_path=None, read_only=True)
# ]
init_container_volume_mounts = [
    k8s.V1VolumeMount(mount_path='/workspace', name='test-volume', read_only=False)
]
init_environments = [k8s.V1EnvVar(name='ACCUTUNING_LOG_LEVEL', value='INFO'), k8s.V1EnvVar(name='ACCUTUNING_WORKSPACE', value='/workspace/experiment_0008/experimentprocess_0037')]

init_container = k8s.V1Container(
    name="init-container",
#     image="harbor.accuinsight.net/accutuning/accutuning/modeler-common:3.0.1",
    image="nginx",    
    env=init_environments,
    volume_mounts=init_container_volume_mounts,

#     command=["bash", "-cx"],
#     args=["echo 10"],
#     args=["sleep 0.03h"],
)

worker = KubernetesPodOperator(
    namespace='default',
#     image="harbor.accuinsight.net/accutuning/accutuning/modeler-common:3.0.1",
    image="nginx",
#     cmds=["sleep", "0.03h"],
    cmds=["bash", "/code/entrypoint.sh"],
  
#    cmds=["bash", "-cx"],
#     arguments=["echo", "10"],
#     labels={"foo": "bar"},
#     secrets=[secret_file, secret_env, secret_all_keys],
#     secrets=[secret_file, secret_env],
#     secrets=[secret_all_keys],
#     ports=[port],
#     env=init_environments,
    volumes=[volume],
    volume_mounts=[volume_mount],
#     volume_mounts=init_container_volume_mounts,
#     env=init_environments,
#     env=[env],
#    env_from=configmaps,
#     env_from=configmaps,
    name="accutuning-test",
    task_id="accutuning",
#     affinity=affinity,
#     is_delete_operator_pod=True,
#     hostnetwork=False,
#     tolerations=tolerations,
    init_containers=[init_container],
#     priority_class_name="medium",
    get_logs=True,
    dag=dag,    
)

# worker = KubernetesPodOperator(namespace='default',
#                           image="harbor.accuinsight.net/accutuning/accutuning/modeler-common:3.0.1",
#                           cmds=["echo","$ACCUTUNING_LOG_LEVEL"],
# #                           arguments=["print('hello world')"],
# #                           labels={"ACCUTUNING_LOG_LEVEL": "INFO","ACCUTUNING_WORKSPACE","/workspace/experiment_0008/experimentprocess_0037"},
#                           name="accutuning-test",
#                           task_id="accutuning",
#                           get_logs=True,
#                           dag=dag
#                           )

end = DummyOperator(task_id='end', dag=dag)


# worker.set_upstream(setting)
# worker.set_upstream(start)
# worker.set_downstream(end)
start >> setting  >> worker >> end
