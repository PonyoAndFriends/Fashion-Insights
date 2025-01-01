import pendulum
from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

@dag(
    schedule="@once",
    start_date=pendulum.datetime(2024, 8, 10, 3, tz=pendulum.timezone("Asia/Seoul")),
    catchup=False,
    tags=["example", "sparkOnK8s"],
)
def spark_on_k8s():

    spark_task = SparkKubernetesOperator(
        task_id="submit_spark_pyspark_job",
        namespace="default",  # Spark Operator가 배포된 네임스페이스
        application_file="/spark_job/spark-pyspark.yaml",  # SparkApplication YAML 파일 경로
        kubernetes_conn_id="kubernetes_default",  # Kubernetes Connection ID
    )

    spark_task

spark_on_k8s()
