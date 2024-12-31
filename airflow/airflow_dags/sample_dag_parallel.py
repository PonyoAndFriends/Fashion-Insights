from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

# 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Python 함수: 1분 동안 sleep
def sleep_task(task_number, **kwargs):
    print(f"Task {task_number} is starting...")
    time.sleep(60)  # 1분 대기
    print(f"Task {task_number} is completed!")

# DAG 정의
with DAG(
    dag_id="parallel_sleep_pythonoperator_tasks",
    default_args=default_args,
    description="Run multiple tasks in parallel, each sleeping for 1 minute",
    schedule_interval=None,
    start_date=datetime(2023, 12, 25),
    catchup=False,
) as dag:

    # 병렬로 실행할 태스크 생성
    tasks = []
    for i in range(5):  # 5개의 태스크를 병렬로 실행
        task = PythonOperator(
            task_id=f"sleep_task_{i+1}",
            python_callable=sleep_task,
            op_kwargs={"task_number": i + 1},
        )
        tasks.append(task)

    # 태스크는 병렬로 실행되므로 의존성 설정 필요 없음
