from airflow import DAG

from datetime import datetime, timedelta
import json
import random

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from custom_operator import DockerExOperator

# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.providers.docker.operators.docker import DockerOperator


default_args = {
    'start_date': datetime(2022, 1, 1),
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(seconds=60),
    'provide_context': True,
    # 'render_template_as_native_obj': True,
    # 'on_failure_callback': on_failure_task,
    # 'on_success_callback': on_success_task,
    # 'execution_timeout': timedelta(seconds=60)
}

# worker_volume_mount = "/home/ubuntu/accutuning.airflow/.workspace:/workspace"
# accutuning_volume_mount = [
#     "/var/run/docker.sock:/var/run/docker.sock",
#     "/home/ubuntu/accutuning.airflow/.workspace:/workspace",
# ]
# environment = {}
# environment.update({
#     'ACCUTUNING_LOG_LEVEL': 'debug',
#     # 'ACCUTUNING_SENTRY_DSN': AccutuningSiteConfiguration.get('ACCUTUNING_SENTRY_DSN_FOR_MODELER'),
# })
# conf = {
#     "accutuning_image": "harbor.accuinsight.net/aiip/aiip-tuning:latest",
#     "worker_image": "harbor.accuinsight.net/aiip/aiip-tuning/modeler-common:latest",
#     # "timeout": timeout,  # TODO timeout에 대한 처리
#     "accutuning_env_vars": json.dumps(get_env_vars()),
#     "worker_env_vars": json.dumps(get_worker_env_vars()),
#     "accutuning_volume_mount": ','.join(accutuning_volume_mount),  # TODO pod 안에서는 volume mount 정보를 구해올 수 있는 방법이 없구나...
#     "worker_volume_mount": worker_volume_mount,  # TODO k8s일 때와, docker에서 ACCUTUNING_DOCKER_VOLUME_MOUNT_MAPS 셋팅시
#     "experiment_id": 13,
#     "experiment_process_type": "preprocess",
#     "experiment_target": "experiment_target",
#     "proceed_next": False,
# }


class TriggerDagRunWithConfigOperator(TriggerDagRunOperator):
    def __init__(self, *args, **kwargs):
        kwargs['wait_for_completion'] = True
        kwargs['poke_interval'] = 1
        kwargs['reset_dag_run'] = True
        kwargs['trigger_dag_id'] = 'ml_run_k8s'
        kwargs['conf'] = dict(experiment_process_type=kwargs['task_id'])
        super().__init__(*args, **kwargs)

    def pre_execute(self, *args, **kwargs):
        from pprint import pprint
        # pprint(kwargs)
        # print(self.conf)
        # print(dir(self))
        conf = kwargs['context'].get('params', {})
        conf.update(self.conf)
        self.conf = conf
        pprint(self.conf)
        return super().pre_execute(*args, **kwargs)


def which_path(*args, **kwargs):
    return kwargs['params'].get('experiment_process_type', 'preprocess')


def which_path_b(*args, **kwargs):
    use_ensemble = kwargs['params'].get('use_ensemble')
    print("use_ensemble = {}".format(use_ensemble))

    if use_ensemble:
        print("dummy_a")
        next_process = 'dummy_a'
    else:
        print("dummy_b")
        next_process = 'dummy_b'

    return kwargs['params'].get('experiment_process_type', next_process)
    # return next_process


with DAG(dag_id='ml_automl', schedule_interval=None, default_args=default_args) as dag:

    parse = TriggerDagRunWithConfigOperator(task_id='parse')
    preprocess = TriggerDagRunWithConfigOperator(task_id='preprocess')
    optuna = TriggerDagRunWithConfigOperator(task_id='optuna')
    # optuna_extra1 = TriggerDagRunWithConfigOperator(task_id='optuna_extra1')
    # optuna_extra2 = TriggerDagRunWithConfigOperator(task_id='optuna_extra2')
    # optuna_extra3 = TriggerDagRunWithConfigOperator(task_id='optuna_extra3')
    ensemble = TriggerDagRunWithConfigOperator(task_id='ensemble')
    deploy = TriggerDagRunWithConfigOperator(task_id='deploy')
    labeling = TriggerDagRunWithConfigOperator(task_id='labeling')
    lb_predict = TriggerDagRunWithConfigOperator(task_id='lb_predict')
    modelstat = TriggerDagRunWithConfigOperator(task_id='modelstat')
    predict = TriggerDagRunWithConfigOperator(task_id='predict')
    cluster = TriggerDagRunWithConfigOperator(task_id='cluster')
    cl_predict = TriggerDagRunWithConfigOperator(task_id='cl_predict')
    dataset_eda = TriggerDagRunWithConfigOperator(task_id='dataset_eda')

    start = DummyOperator(task_id='start')
    start_branch = BranchPythonOperator(task_id='branch', python_callable=which_path)
    end = DummyOperator(task_id='end', trigger_rule='one_success')

    dummy_a = DummyOperator(task_id='dummy_a')
    dummy_b = DummyOperator(task_id='dummy_b')
    
    ensemble_branch = BranchPythonOperator(task_id='ensemble_branch', python_callable=which_path_b)

    start >> start_branch >> [parse, deploy, labeling, lb_predict, modelstat, predict, cluster, cl_predict, dataset_eda] >> end
    # start_branch >> preprocess >> [optuna, optuna_extra1, optuna_extra2, optuna_extra3] >> ensemble >> deploy >> end

    start_branch >> preprocess >> optuna >> ensemble_branch

    ensemble_branch >> dummy_a >> ensemble >> deploy >> end
    ensemble_branch >> dummy_b >> deploy >> end
