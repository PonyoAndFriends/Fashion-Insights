from airflow import DAG
from airflow.providers.amazon.aws.operators.eks import EKSPodOperator
from datetime import datetime
from airflow.models import Variable

# 고유 ID 생성
# import uuid
unique_id = 'test-1'  # str(uuid.uuid4())[:8]

MASTER_SERVICE_NAME = f"spark-master"
MASTER_DNS = f"{MASTER_SERVICE_NAME}.default.svc.cluster.local"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="eks_spark_s3",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Spark Master Pod
    spark_master = EKSPodOperator(
        task_id="spark_master",
        cluster_name="ponyo-test-cluster",
        pod_name=f"spark-master-{unique_id}",
        namespace="airflow",
        image="coffeeisnan/spark-master:latest",
        cmds=["/bin/bash", "-c"],
        arguments=[
            "/opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.master.Master",
        ],
        labels={"app": f"spark-{unique_id}", "role": "master"},
        is_delete_operator_pod=False,
    )

    # Spark Worker Pod
    spark_worker = EKSPodOperator(
        task_id="spark_worker",
        cluster_name="ponyo-test-cluster",
        pod_name=f"spark-worker-{unique_id}",
        namespace="airflow",
        image="coffeeisnan/spark-worker:latest",
        cmds=["/bin/bash", "-c"],
        arguments=[
            f"/opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://{MASTER_DNS}:7077"
        ],
        labels={"app": f"spark-{unique_id}", "role": "worker"},
        is_delete_operator_pod=False,
    )

    # PySpark 작업 실행
    spark_submit = EKSPodOperator(
    task_id="spark_submit",
    cluster_name="ponyo-test-cluster",
    pod_name="spark-submit-pod",
    namespace="airflow",
    image="coffeeisnan/spark-master:latest",
    cmds=["/bin/bash", "-c"],
    arguments=[
        "/opt/bitnami/spark/bin/spark-submit",
        f"--master spark://{MASTER_DNS}:7077",
        "--deploy-mode cluster",
        "--executor-memory 2g",
        "--total-executor-cores 4",
        "--conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        rf"--conf spark.hadoop.fs.s3a.access.key={Variable.get('aws_access_key_id')}",
        rf"--conf spark.hadoop.fs.s3a.secret.key={Variable.get('aws_secret_access_key')}",
        "--conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com",
        "/opt/spark/jobs/spark_s3_job.py",
    ],
    labels={"app": f"spark-{unique_id}", "role": "submit"},
    is_delete_operator_pod=True,
    )

    # DAG 의존성 설정
    spark_master >> spark_worker >> spark_submit
