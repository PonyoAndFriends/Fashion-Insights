from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

# 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# DAG 정의
with DAG(
    dag_id="parallel_sleep_with_k8s_operator",
    default_args=default_args,
    description="Run multiple tasks in parallel using KubernetesPodOperator",
    schedule_interval=None,
    start_date=datetime(2023, 12, 25),
    catchup=False,
) as dag:

    # 병렬로 실행할 태스크 생성
    tasks = []
    for i in range(5):  # 병렬로 5개의 태스크 실행
        task = KubernetesPodOperator(
            task_id=f"sleep_task_{i+1}",
            namespace="airflow",
            name=f"sleep-task-{i+1}",
            image="busybox",  # 간단한 BusyBox 이미지를 사용
            cmds=["sh", "-c"],
            arguments=["echo 'Task started'; sleep 60; echo 'Task completed'"],
            is_delete_operator_pod=True,  # 태스크 완료 후 Pod 삭제
            get_logs=True,  # Pod 로그를 Airflow에서 볼 수 있도록 설정
        )
        tasks.append(task)

    # 병렬 실행
    for task in tasks:
        task
