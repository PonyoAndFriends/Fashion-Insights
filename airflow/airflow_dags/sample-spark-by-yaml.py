from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="submit_pyspark_job_to_operator",
    default_args=default_args,
    description="Submit PySpark job using Spark Operator",
    schedule_interval=None,
    start_date=datetime(2023, 12, 25),
    catchup=False,
) as dag:

    # Spark Application 제출
    submit_spark_job = SparkKubernetesOperator(
        task_id="submit_pyspark_job",
        namespace="spark",
        application_file="/opt/airflow/dags/repo/airflow/airflow_dags/spark_application.yaml",  # Spark Application YAML 경로
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    # Spark Job 상태 확인
    monitor_spark_job = SparkKubernetesSensor(
        task_id="monitor_pyspark_job",
        namespace="spark",
        application_name="{{ task_instance.xcom_pull(task_ids='submit_pyspark_job')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default",
        poke_interval=10,
        timeout=600,
    )

    submit_spark_job >> monitor_spark_job
