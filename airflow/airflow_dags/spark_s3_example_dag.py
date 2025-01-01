from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

# 기본 DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# sprark 코드 파일 경로
PYSPARK_FILE = "/opt/spark/jobs/s3_example.py"

# DAG 정의
with DAG(
    dag_id='spark_s3_example',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Spark 파드 실행
    spark_task = KubernetesPodOperator(
        namespace='airflow',
        image='coffeeisnan/spark_test_image:3',
        cmds=["/opt/bitnami/spark/bin/spark-submit"],
        arguments=[
            '--class', 'org.apache.spark.examples.S3Example',
            '--master', 'k8s://https://kubernetes.default.svc',
            '--deploy-mode', 'cluster',
            '--conf', 'spark.kubernetes.namespace=airflow',
            '--conf', 'spark.executor.instances=2',
            '--conf', 'spark.kubernetes.authenticate.driver.serviceAccountName=spark-service-account',
            PYSPARK_FILE,  # Spark 애플리케이션
            's3a://source-bucket-hs/input-data.json',  # 입력 파일
            's3a://destination-bucket-hs/output-data.json'  # 출력 파일
        ],
        container_resources={
            "limit_memory": "4Gi",
            "limit_cpu": "2",
        },
        name='spark-s3-task',
        task_id='spark_task',
        get_logs=True,
        is_delete_operator_pod=True,
    )