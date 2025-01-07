# Example Usage in a DAG
from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from custom_operators.custom_modules.otherapis_dependencies import OTHERAPI_DEFAULT_ARGS
from custom_operators.k8s_spark_job_submit_operator import SparkApplicationOperator

default_args = OTHERAPI_DEFAULT_ARGS

with DAG(
    dag_id="custom_spark_operator_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    trigger_spark_application = SparkApplicationOperator(
        task_id="submit_spark_application_task",
        name="s3-processing-job-1",  # 필수 필드, EKS 안에서 실행하는 스파크 잡의 이름, 중복 불가(커스텀 오퍼레이터 안에 같은 이름 존재 시삭제 로직이 자동으로 돌게 되어있음)
        namespace="default",
        image="coffeeisnan/spark-job:latest",
        main_application_file="local:///opt/spark/jobs/pyspark_example.py",  # 필수 필드, spark에서 실행할 코드의 경로
        spark_version="3.5.4",
        driver_config={
            "cores": 1,
            "coreLimit": "1200m",
            "memory": "1g",
            "serviceAccount": "spark-driver-sa",
        },
        executor_config={
            "cores": 1,
            "instances": 2,  # executor의 pod 개수
            "memory": "1g",
        },
        deps={
            "jars": [
                "local:///opt/spark/user-jars/hadoop-aws-3.3.1.jar",
                "local:///opt/spark/user-jars/aws-java-sdk-bundle-1.11.901.jar",
            ],
        },
        spark_conf={
            "spark.hadoop.fs.s3a.access.key": Variable.get("aws_access_key_id"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("aws_secret_access_key"),
            "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
            "spark.kubernetes.driver.deleteOnTermination": "true",
            "spark.kubernetes.executor.deleteOnTermination": "true",
        },
    )
