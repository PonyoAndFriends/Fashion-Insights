from airflow import DAG
from airflow.operators.python import PythonOperator
from kubernetes import client, config
from datetime import datetime
from airflow.models import Variable

def submit_spark_application():
    # Kubernetes 클라이언트 설정
    config.load_incluster_config()  # EKS 내부에서 실행 중인 경우
    api_instance = client.CustomObjectsApi()

    # SparkApplication CRD 정의
    spark_application = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": "s3-processing-job-4",
            "namespace": "default",
        },
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "image": "coffeeisnan/spark-job:latest",
            "imagePullPolicy": "Always",
            "mainApplicationFile": "local:///opt/spark/jobs/pyspark_example.py",
            "sparkVersion": "3.5.4",
            "restartPolicy": {
                "type": "Never",
            },
            "driver": {
                "cores": 1,
                "coreLimit": "1200m",
                "memory": "1g",
                "serviceAccount": "spark-driver-sa",
            },
            "executor": {
                "cores": 1,
                "instances": 2,
                "memory": "1g",
            },
            "deps": {
                "jars": [
                    "local:///opt/spark/user-jars/hadoop-aws-3.3.1.jar",
                    "local:///opt/spark/user-jars/aws-java-sdk-bundle-1.11.901.jar",
                ],
            },
            "sparkConf": {
                "spark.hadoop.fs.s3a.access.key": Variable.get("aws_access_key_id"),
                "spark.hadoop.fs.s3a.secret.key": Variable.get("aws_secret_access_key"),
                "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
            },
        },
    }

    # SparkApplication 생성
    api_instance.create_namespaced_custom_object(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        namespace="default",
        plural="sparkapplications",
        body=spark_application,
    )

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="spark_operator_trigger_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    trigger_spark_application = PythonOperator(
        task_id="submit_spark_application",
        python_callable=submit_spark_application,
    )
