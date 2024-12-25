from airflow import DAG
from airflow.\
    providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# DAG 정의
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    's3_spark_dict_task',
    default_args=default_args,
    description='A simple DAG to run Spark job that writes a dictionary to S3',
    schedule_interval=None,  # 수동으로 실행
    start_date=datetime(2024, 12, 20),
    catchup=False,
) as dag:

    # SparkSubmitOperator로 Spark 작업 실행
    spark_task = SparkSubmitOperator(
        task_id='spark_submit_task',
        conn_id='spark_k8s',  # K8s와 Spark 연결
        application='/path/to/spark_job.py',  # Spark 코드 파일 경로
        name='s3-spark-job',
        conf={'spark.executor.memory': '2g', 'spark.driver.memory': '2g'},
        jars='s3://path/to/dependencies.jar',  # 필요시 JAR 파일 추가
        execution_timeout=timedelta(minutes=10),
    )
