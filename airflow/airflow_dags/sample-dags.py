from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

def test_python_operator(**kwargs):
    print("PythonOperator Task started")
    time.sleep(60)  # 60초 동안 대기, Pod 상태를 확인할 시간 제공
    print("PythonOperator Task completed")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='test_python_operator_pod_creation',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 12, 25),
    catchup=False,
) as dag:

    python_operator_task = PythonOperator(
        task_id='test_python_operator',
        python_callable=test_python_operator,
    )
