from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import time

def example_task(**kwargs):
    # 태스크에서 실행할 로직
    print("Hello from Airflow Worker Pod! - python operator")
    time.sleep(180)
    print("Running on a KubernetesExecutor.")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='example_kubernetes_executor_dag',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 12, 25),
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id='example_task',
        python_callable=example_task,
    )
