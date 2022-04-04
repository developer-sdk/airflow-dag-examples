import datetime
from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator


args = {'owner': 'deploy',
        'start_date': datetime.datetime(2022, 4, 4),
        'retries': 0,
        'catchup': False}


def func_python(**context):
    print("## func_python run ##")


def fuct_failure(params):
    print("### fuct_failure ###")


with DAG(dag_id='python_task', default_args=args, schedule_interval=None) as dag:

    t1_python = PythonOperator(task_id='t1_python', python_callable=func_python, on_failure_callback=fuct_failure, dag=dag)
