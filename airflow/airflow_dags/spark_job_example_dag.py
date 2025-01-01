from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

# PySpark 파일 경로 (DAG의 dags/ 폴더 기준)
PYSPARK_FILE = "/opt/airflow/dags/repo/airflow/airflow_dags/spark_job/example_job.py"

# DAG 정의
with DAG(
    dag_id="real_test_dag",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    spark_job = KubernetesPodOperator(
        task_id="spark_cluster_mode_job",
        namespace="airflow",  # 작업 실행할 네임스페이스
        name="spark-cluster-job",
        image="bitnami/spark:3.5.4",  # Spark 3.5.4 이미지
        cmds=["/opt/bitnami/spark/bin/spark-submit"],  # Spark-submit 명령어
        arguments=[
            "--master", "k8s://https://kubernetes.default.svc",  # Kubernetes API 주소
            "--deploy-mode", "cluster",  # 클러스터 모드
            "--conf", "spark.kubernetes.container.image=bitnami/spark:3.5.4",  # Spark 워커 이미지
            "--conf", "spark.executor.instances=3",  # 워커 파드 개수
            "--conf", "spark.executor.memory=2g",  # 워커 메모리
            "--conf", "spark.executor.cores=1",  # 워커 CPU 코어
            PYSPARK_FILE,  # PySpark 코드 경로
        ],
        container_resources={
            "limit_memory": "4Gi",
            "limit_cpu": "2",
        },
        is_delete_operator_pod=True,  # 작업 완료 후 Pod 삭제
    )
