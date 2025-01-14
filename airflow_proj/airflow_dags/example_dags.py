from airflow import DAG
from airflow.operators.python import PythonOperator
from kubernetes import client, config
from datetime import datetime
from airflow.models import Variable


spark_args = [
    "--input-path",
    "s3a://your-bucket/input/",
    "--output-path",
    "s3a://your-bucket/output/",
]


def submit_spark_application(spark_app_name, pyspark_py_path, spark_args=None):
    # Kubernetes 클라이언트 설정
    config.load_incluster_config()  # EKS 내부에서 실행 중인 경우
    api_instance = client.CustomObjectsApi()

    spark_app_name = "s3-processing-job"

    # 기존 SparkApplication 삭제
    try:
        api_instance.delete_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace="default",
            plural="sparkapplications",
            name=spark_app_name,
        )
    except client.exceptions.ApiException as e:
        if e.status != 404:
            raise

    # SparkApplication CRD 정의
    spark_application = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": spark_app_name,
            "namespace": "default",
        },
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "image": "coffeeisnan/spark-job:latest",
            "imagePullPolicy": "Always",
            "mainApplicationFile": f"local:///opt/spark/jobs/{pyspark_py_path}",
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
                "memory": "2g",
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
                "spark.kubernetes.driver.deleteOnTermination": "true",
                "spark.kubernetes.executor.deleteOnTermination": "true",
            },
        },
    }

    # 아규먼트를 조건부로 추가
    if spark_args:
        spark_application["spec"]["arguments"] = spark_args

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
