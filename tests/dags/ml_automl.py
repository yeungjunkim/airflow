from airflow import DAG

from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
import json

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
        kwargs['trigger_dag_id'] = 'ml_run_docker'
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

        if json.loads(self.conf['accutuning_env_vars'])['ACCUTUNING_K8S_USE'] == '1':
            if self.conf['experiment_process_type'] == 'optuna_monitor' or \
                    self.conf['experiment_process_type'] == 'ensemble_monitor':
                trigger_dag_id = 'ml_run_k8s_monitor'
            else:
                trigger_dag_id = 'ml_run_k8s'
        else:
            if self.conf['experiment_process_type'].endswith('_monitor'):
                trigger_dag_id = 'accutuning_command_on_docker'
            else:
                trigger_dag_id = 'ml_run_docker'

        self.trigger_dag_id = trigger_dag_id

        return super().pre_execute(*args, **kwargs)


def which_path(*args, **kwargs):
    next_process = kwargs['params'].get('experiment_process_type', 'preprocess')
    print(" start_branch, next_process = {}".format(next_process))
    return next_process


def which_path2(*args, **kwargs):
    use_ensemble = kwargs['params'].get('experiment_config', {}).get('experiment', {}).get('use_ensemble')

    if use_ensemble:
        print("ensemble")
        next_process = ['ensemble', 'ensemble_monitor']
    else:
        next_process = 'no_ensemble'
    print(" use_ensemble = {}, next_process = {}".format(use_ensemble, next_process))
    return next_process


def which_path3(*args, **kwargs):
    proceed_next = kwargs['params'].get('proceed_next')

    if proceed_next:
        next_process = 'yes_batch_automl'
    else:
        next_process = 'parse_only'
    print(" proceed_next = {}, next_process = {}".format(proceed_next, next_process))
    return next_process


def _build(task_id):
    with TaskGroup(group_id=task_id) as tg:
        preprocess = TriggerDagRunWithConfigOperator(task_id='preprocess')
        optuna = TriggerDagRunWithConfigOperator(task_id='optuna')

        def which_en_path(*args, **kwargs):
            # ['batch_automl.no_ensemble', 'batch_automl.ensemble']
            use_ensemble = kwargs['params'].get('experiment_config', {}).get('experiment', {}).get('use_ensemble')
            print(f"use_ensemble = {use_ensemble}")
            print("use_ensemble = {}".format(kwargs['params'].get('experiment_config', {}).get('experiment', {}).get('use_ensemble')))

            if use_ensemble:
                next_process = 'batch_automl.ensemble'
            else:
                next_process = 'batch_automl.no_ensemble'
            print(" use_ensemble = {}, next_process = {}".format(use_ensemble, next_process))
            return next_process

        ensemble_branch = BranchPythonOperator(task_id='ensemble_branch', python_callable=which_en_path)
        no_ensemble = DummyOperator(task_id='no_ensemble')
        ensemble = TriggerDagRunWithConfigOperator(task_id='ensemble')

        deploy_auto = TriggerDagRunWithConfigOperator(
            task_id='deploy_auto',
            trigger_rule='one_success',
            conf=dict(experiment_process_type='deploy'))
        preprocess >> optuna >> ensemble_branch >> [ensemble, no_ensemble] >> deploy_auto

    return tg


with DAG(dag_id='ml_automl', schedule_interval=None, default_args=default_args) as dag:

    parse = TriggerDagRunWithConfigOperator(task_id='parse')
    parse_cluster = TriggerDagRunWithConfigOperator(task_id='parse_cluster', conf=dict(experiment_process_type='parse'))
    parse_labeling = TriggerDagRunWithConfigOperator(task_id='parse_labeling', conf=dict(experiment_process_type='parse'))
    preprocess = TriggerDagRunWithConfigOperator(task_id='preprocess', trigger_rule='one_success')
    optuna = TriggerDagRunWithConfigOperator(task_id='optuna')
    optuna_monitor = TriggerDagRunWithConfigOperator(task_id='optuna_monitor')
    # optuna_extra1 = TriggerDagRunWithConfigOperator(task_id='optuna_extra1')
    # optuna_extra2 = TriggerDagRunWithConfigOperator(task_id='optuna_extra2')
    # optuna_extra3 = TriggerDagRunWithConfigOperator(task_id='optuna_extra3')
    ensemble = TriggerDagRunWithConfigOperator(task_id='ensemble')
    ensemble_monitor = TriggerDagRunWithConfigOperator(task_id='ensemble_monitor')
    deploy = TriggerDagRunWithConfigOperator(task_id='deploy')
    deploy_auto = TriggerDagRunWithConfigOperator(task_id='deploy_auto', trigger_rule='one_success', conf=dict(experiment_process_type='deploy'))
    # deploy_auto_with_ensemble = TriggerDagRunWithConfigOperator(
    #     task_id='deploy_auto_with_ensemble', conf=dict(target=None, experiment_process_type='deploy'))
    ml_labeling = TriggerDagRunWithConfigOperator(task_id='ml_labeling', conf=dict(experiment_process_type='labeling'))
    # lb_predict = TriggerDagRunWithConfigOperator(task_id='lb_predict')
    modelstat = TriggerDagRunWithConfigOperator(task_id='modelstat')
    predict = TriggerDagRunWithConfigOperator(task_id='predict')
    ml_cluster = TriggerDagRunWithConfigOperator(task_id='ml_cluster', conf=dict(experiment_process_type='cluster'))
    # cl_predict = TriggerDagRunWithConfigOperator(task_id='cl_predict')
    dataset_eda = TriggerDagRunWithConfigOperator(task_id='dataset_eda')
    # closing = TriggerDagRunWithConfigOperator(task_id='closing', trigger_rule="none_skipped")

    start = DummyOperator(task_id='start')
    start_branch = BranchPythonOperator(task_id='branch', python_callable=which_path, do_xcom_push=True)
    end = DummyOperator(task_id='end', trigger_rule='one_success')

    ensemble_branch = BranchPythonOperator(task_id='ensemble_branch', python_callable=which_path2)
    batch_branch = BranchPythonOperator(task_id='batch_branch', python_callable=which_path3)
    no_ensemble = DummyOperator(task_id='no_ensemble')
    cluster = DummyOperator(task_id='cluster')
    labeling = DummyOperator(task_id='labeling')

    yes_batch_automl = DummyOperator(task_id='yes_batch_automl')
    parse_only = DummyOperator(task_id='parse_only')
    batch_automl = _build('batch_automl')

    start >> start_branch >> [deploy, modelstat, predict, dataset_eda] >> end

    start_branch >> preprocess >> optuna >> ensemble_branch >> [ensemble, ensemble_monitor, no_ensemble]
    preprocess >> optuna_monitor
    ensemble >> deploy_auto
    no_ensemble >> deploy_auto
    deploy_auto >> end
    start_branch >> parse >> batch_branch >> parse_only >> end
    start_branch >> parse >> batch_branch >> yes_batch_automl >> batch_automl >> end
    start_branch >> cluster >> parse_cluster >> ml_cluster >> end
    start_branch >> labeling >> parse_labeling >> ml_labeling >> end
