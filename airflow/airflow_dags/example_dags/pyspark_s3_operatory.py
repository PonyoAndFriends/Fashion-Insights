from airflow import DAG
from airflow.operators.python import PythonOperator
from kubernetes import client, config
from datetime import datetime
from airflow.models import Variable
from datetime import datetime

def submit_spark_application():
    config.load_incluster_config()
    api_instance = client.CustomObjectsApi()
    now = datetime.now()
    now_string = now.strftime("%Y%m%d%H%M%S")
    spark_app_name = f"musinsa-processing-job-{now_string}"

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

    spark_application = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": spark_app_name, "namespace": "default"},
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "image": "coffeeisnan/spark-job:latest",
            "imagePullPolicy": "Always",
            "mainApplicationFile": "local:///opt/spark/jobs/pyspark_s3_example.py",
            "sparkVersion": "3.5.4",
            "restartPolicy": {"type": "Never"},
            "driver": {
                "cores": 1,
                "memory": "1g",
                "serviceAccount": "spark-driver-sa",
            },
            "executor": {
                "cores": 1,
                "instances": 2,
                "memory": "1g",
            },
            "sparkConf": {
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.access.key": Variable.get("aws_access_key_id"),
                "spark.hadoop.fs.s3a.secret.key": Variable.get("aws_secret_access_key"),
                "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
                "spark.sql.parquet.compression.codec": "snappy",
            },
            "deps": {
                "jars": [
                    "local:///opt/spark/user-jars/hadoop-aws-3.3.1.jar",
                    "local:///opt/spark/user-jars/aws-java-sdk-bundle-1.11.901.jar",
                ]
            },
        },
    }

    api_instance.create_namespaced_custom_object(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        namespace="default",
        plural="sparkapplications",
        body=spark_application,
    )

default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1), "retries": 1}

with DAG(
    "musinsa_spark_operator",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    submit_application = PythonOperator(
        task_id="submit_spark_application", python_callable=submit_spark_application
    )
