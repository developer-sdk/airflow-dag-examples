import datetime
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator


args = {'owner': 'deploy',
        'start_date': datetime.datetime(2022, 3, 28),
        'retries': 0,
        'catchup': False
        }


def func_a(**context):
    print("AAA")
    task_instance = context['task_instance']
    task_instance.xcom_push(key='is_true', value=False)

def func_b(**context):
    print("BBB")

def func_c(**context):
    print("CCC")

def fuct_failure(params):
    print("### FAILURE ###")

def branch_func(**context):
    task_instance = context['task_instance']
    is_true = task_instance.xcom_pull(key='is_true')

    if is_true:
        return 't1_b'
    else:
        return 't1_c'

with DAG(dag_id='xcom_test', default_args=args, schedule_interval=None, concurrency=40, max_active_runs=16) as dag:

    t1_create_cluster = PythonOperator(task_id='t1_a', python_callable=func_a, on_failure_callback=fuct_failure, dag=dag)
    t2_create_cluster = PythonOperator(task_id='t1_b', python_callable=func_b, on_failure_callback=fuct_failure, dag=dag)
    t3_create_cluster = PythonOperator(task_id='t1_c', python_callable=func_c, on_failure_callback=fuct_failure, dag=dag)

    branch_op = BranchPythonOperator(task_id='branch_task', python_callable=branch_func, dag=dag)

    t1_create_cluster >> branch_op
    branch_op >> t2_create_cluster
    branch_op >> t3_create_cluster
