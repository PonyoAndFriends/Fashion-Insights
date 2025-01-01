from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

unique_id = "test-1"
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
    spark_master = KubernetesPodOperator(
        task_id="spark_master",
        namespace="airflow",
        image="coffeeisnan/spark-master:latest",
        cmds=["/bin/bash", "-c"],
        arguments=[
            "/opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.master.Master & sleep infinity"
        ],
        labels={"app": f"spark-{unique_id}", "role": "master"},
        name=f"spark-master-{unique_id}",
        is_delete_operator_pod=False,
        get_logs=True,
        startup_timeout_seconds=300,
        wait_for_completion=False,  # Ready 상태에서 완료로 간주
    )

    # Spark Worker Pod
    spark_worker = KubernetesPodOperator(
        task_id="spark_worker",
        namespace="airflow",
        image="coffeeisnan/spark-worker:latest",
        cmds=["/bin/bash", "-c"],
        arguments=[
            "while ! nc -zv spark-master.default.svc.cluster.local 7077; do sleep 1; done; "
            "/opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master.default.svc.cluster.local:7077"
        ],
        labels={"app": f"spark-{unique_id}", "role": "worker"},
        name=f"spark-worker-{unique_id}",
        is_delete_operator_pod=False,
        get_logs=True,
    )

    # PySpark 작업 실행
    spark_submit = KubernetesPodOperator(
        task_id="spark_submit",
        namespace="airflow",
        image="coffeeisnan/spark-master:latest",
        cmds=["/bin/bash", "-c"],
        arguments=[
            "/opt/bitnami/spark/bin/spark-submit",
            f"--master spark://{MASTER_DNS}:7077",
            "--deploy-mode cluster",
            "--executor-memory 2g",
            "--total-executor-cores 4",
            "/opt/spark/jobs/spark_s3_job.py",
        ],
        labels={"app": f"spark-{unique_id}", "role": "submit"},
        name=f"spark-submit-{unique_id}",
        is_delete_operator_pod=True,
        get_logs=True,
    )

    # DAG 의존성 설정
    spark_master >> spark_worker >> spark_submit
