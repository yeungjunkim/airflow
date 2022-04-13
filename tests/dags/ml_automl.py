from airflow import DAG

from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


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


class TriggerDagRunWithConfigOperator(TriggerDagRunOperator):
    def __init__(self, *args, **kwargs):
        kwargs['wait_for_completion'] = True
        kwargs['poke_interval'] = 1
        kwargs['reset_dag_run'] = True
        kwargs['conf'] = kwargs.get('conf') or dict(experiment_process_type=kwargs['task_id'])
        kwargs['trigger_dag_id'] = 'ml_run_k8s'
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
        print("self.conf = {}".format(self.conf))
        print("self.conf['accutuning_env_vars'] = {}".format(self.conf['accutuning_env_vars']))
        if self.conf['accutuning_env_vars']:
            trigger_dag_id = 'ml_run_k8s'
        else:
            trigger_dag_id = 'ml_run_docker'

        print("trigger_dag_id = {}".format(trigger_dag_id))

        self.trigger_dag_id = trigger_dag_id
        print("self.trigger_dag_id = {}".format(self.trigger_dag_id))

        return super().pre_execute(*args, **kwargs)


def which_path(*args, **kwargs):
    return kwargs['params'].get('experiment_process_type', 'preprocess')


def which_path2(*args, **kwargs):
    use_ensemble = kwargs['params'].get('use_ensemble')

    if use_ensemble:
        print("ensemble")
        next_process = 'ensemble'
    else:
        next_process = 'no_ensemble'
    print(" use_ensemble = {}, next_process = {}".format(use_ensemble, next_process))
    return next_process


with DAG(dag_id='ml_automl', schedule_interval=None, default_args=default_args) as dag:

    parse = TriggerDagRunWithConfigOperator(task_id='parse')
    preprocess = TriggerDagRunWithConfigOperator(task_id='preprocess')
    optuna = TriggerDagRunWithConfigOperator(task_id='optuna')
    # optuna_extra1 = TriggerDagRunWithConfigOperator(task_id='optuna_extra1')
    # optuna_extra2 = TriggerDagRunWithConfigOperator(task_id='optuna_extra2')
    # optuna_extra3 = TriggerDagRunWithConfigOperator(task_id='optuna_extra3')
    ensemble = TriggerDagRunWithConfigOperator(task_id='ensemble')
    deploy = TriggerDagRunWithConfigOperator(task_id='deploy')
    deploy_auto = TriggerDagRunWithConfigOperator(task_id='deploy_auto', conf=dict(target=None, experiment_process_type='deploy'))
    deploy_auto_with_ensemble = TriggerDagRunWithConfigOperator(
        task_id='deploy_auto_with_ensemble', conf=dict(target=None, experiment_process_type='deploy'))
    labeling = TriggerDagRunWithConfigOperator(task_id='labeling')
    lb_predict = TriggerDagRunWithConfigOperator(task_id='lb_predict')
    modelstat = TriggerDagRunWithConfigOperator(task_id='modelstat')
    predict = TriggerDagRunWithConfigOperator(task_id='predict')
    cluster = TriggerDagRunWithConfigOperator(task_id='cluster')
    cl_predict = TriggerDagRunWithConfigOperator(task_id='cl_predict')
    dataset_eda = TriggerDagRunWithConfigOperator(task_id='dataset_eda')
    # closing = TriggerDagRunWithConfigOperator(task_id='closing', trigger_rule="none_skipped")

    start = DummyOperator(task_id='start')
    start_branch = BranchPythonOperator(task_id='branch', python_callable=which_path, do_xcom_push=True)
    end = DummyOperator(task_id='end', trigger_rule='one_success')

    ensemble_branch = BranchPythonOperator(task_id='ensemble_branch', python_callable=which_path2)
    no_ensemble = DummyOperator(task_id='no_ensemble')

    start >> start_branch >> [parse, deploy, labeling, lb_predict, modelstat, predict, cluster, cl_predict, dataset_eda] >> end
    # start_branch >> preprocess >> [optuna, optuna_extra1, optuna_extra2, optuna_extra3] >> ensemble >> deploy >> end

    # ensemble branch에서 ensemble이 아니면 deploy_auto를 수행하지 않는 문제;
    start_branch >> preprocess >> optuna >> ensemble_branch >> ensemble >> deploy_auto_with_ensemble >> end
    start_branch >> preprocess >> optuna >> ensemble_branch >> no_ensemble >> deploy_auto >> end
