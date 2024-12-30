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
    dag_id="submit_spark_job",
    default_args=default_args,
    description="Submit Spark job to Kubernetes cluster",
    schedule_interval=None,
    start_date=datetime(2023, 12, 25),
    catchup=False,
) as dag:

    # Spark Job을 실행하는 KubernetesPodOperator
    submit_spark_job = KubernetesPodOperator(
        task_id="submit_spark_job",
        namespace="airflow",  # Airflow가 실행 중인 네임스페이스
        name="spark-sleep-job",
        image="bitnami/spark:3.3.1",  # Spark 이미지
        cmds=["/opt/bitnami/spark/bin/spark-submit"],
        arguments=[
            "--master", "k8s://https://kubernetes.default.svc",
            "--deploy-mode", "cluster",
            "--name", "spark-sleep-job",
            "--conf", "spark.kubernetes.namespace=airflow",
            "--conf", "spark.executor.instances=1",
            "--conf", "spark.kubernetes.container.image=bitnami/spark:3.3.1",
            "/opt/airflow/dags/spark_job/spark_sleep.py",
        ],
        is_delete_operator_pod=True,  # 작업 완료 후 Pod 삭제
        get_logs=True,
    )

    submit_spark_job
