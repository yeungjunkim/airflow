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
    'accutuning-rest-api-call', default_args=default_args, schedule_interval=timedelta(minutes=10))

start = DummyOperator(task_id='start', dag=dag)

# cmd = 'eval $(/usr/local/bin/minikube -p /usr/local/bin/minikube docker-env)'
cmd = 'pwd;ls -al;uname -a'
setting = BashOperator(task_id='setting', bash_command=cmd, dag=dag)

#  docker run -it 
#     -e ACCUTUNING_LOG_LEVEL=INFO 
#     -e ACCUTUNING_WORKSPACE=/workspace/experiment_0008/experimentprocess_0037 
#     -v /Users/yeongjunkim/dev/accutuning_gitlab/accutuning/.workspace:/workspace 
#         accutuning/modeler-common:latest 

# secret_all_keys = Secret('env', None, 'default-token-8cv8w')

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

port = k8s.V1ContainerPort(name='http', container_port=80)

init_container_volume_mounts = [
    k8s.V1VolumeMount(mount_path='/workspace', name='test-volume', read_only=False)
]
# init_environments = [k8s.V1EnvVar(name='ACCUTUNING_LOG_LEVEL', value='INFO'), k8s.V1EnvVar(name='ACCUTUNING_WORKSPACE', value='/workspace/experiment_0008/experimentprocess_0037')]


init_container = k8s.V1Container(
    name="init-container",
    image="pooh97/accutuning:latest",    
    command=["bash", "-cx"],
    args=["pwd;ls -al /workspace"],
    volume_mounts=init_container_volume_mounts,
)
    
worker = KubernetesPodOperator(
    namespace='default',
    image="pooh97/accutuning:latest",    
    volumes=[volume],
    volume_mounts=[volume_mount],
    name="accutuning-test",
    task_id="accutuning",
    init_containers=[init_container],
    env_vars={'ACCUTUNING_LOG_LEVEL': '{{dag_run.conf["ACCUTUNING_LOG_LEVEL"] if dag_run else "" }}', 'ACCUTUNING_WORKSPACE':'{{dag_run.conf["ACCUTUNING_WORKSPACE"] if dag_run else "" }}'},
    get_logs=True,
    dag=dag,    
)

end = DummyOperator(task_id='end', dag=dag)



start >> setting  >> worker >> end
