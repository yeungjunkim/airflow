# subdag.py

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
def random_subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name), # subdag의 id는 이와같은 컨벤션으로 쓴답니다.
        default_args=args,
        schedule_interval=None, # 값을 넣어주어야 합니다.
    )
    union =DummyOperator(
        task_id='%s-union' % (child_dag_name),
        default_args=args,
        dag=dag_subdag
        )

    for i in range(2):
        globals()['process_a'] = BashOperator(
            task_id='%s-task_A-%s' % (child_dag_name, i + 1),
            default_args=args,
            bash_command='echo "does it work?"',
            dag=dag_subdag,
        )
        globals()['process_b'] = BashOperator(
            task_id='%s-task_B-%s' % (child_dag_name, i + 1),
            default_args=args,
            bash_command='date',
            dag=dag_subdag,
        )

        process_a >> process_b >> union

    return dag_subdag
