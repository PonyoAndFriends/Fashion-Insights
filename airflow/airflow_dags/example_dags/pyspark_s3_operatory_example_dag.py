from airflow import DAG
from datetime import datetime
from custom_operators.eks_spark_submit_operator import SparkSubmitOperator

default_args = {"owner": "team3-2@example.com", "start_date": datetime(2024, 1, 1), "retries": 1}

with DAG(
    dag_id="custom_spark_operator_example_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    # spark job 제출을 위한 커스텀 오퍼레이터를 사용하는 예제 코드, 필수 파라미터는 task_id와 name 뿐
    submit_spark_task = SparkSubmitOperator(
        task_id="submit_spark_application_task",
        name="example-processing-job",
        namespace="default",  # default 네임 스페이스를 추천
        image="coffeeisnan/spark-job:latest", 
        main_application_file_name="pyspark_s3_example.py",
        driver_cores=2,
        driver_memory="2g",
        executor_cores=1,
        executor_memory="1g",
        executor_instances=3,
        spark_conf={"spark.executor.memoryOverhead": "512m"},
    )
