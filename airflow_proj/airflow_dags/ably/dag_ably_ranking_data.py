from airflow import DAG
from airflow.operators.python import PythonOperator
from ably.ably_modules.k8s_spark_job_submit_operator import submit_spark_application
from ably.ably_modules.k8s_custom_python_pod_operator import CustomKubernetesPodOperator
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from ably.ably_modules.ably_dependencies import (
    ABLYAPI_DEFAULT_ARGS,
)
import logging


# DAG 정의
dag = DAG(
    "ably_ranking_data",  # DAG ID
    default_args=ABLYAPI_DEFAULT_ARGS,
    description="Collect the Ably rank data and goods_sno, then upload the JSON file to S3.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


raking_goods_data_task = CustomKubernetesPodOperator(
    task_id=f"ranking_goods_data_task",
    namespace="airflow",
    script_path="/python_scripts/ably/ably_ranking_data.py",
    cpu_limit="1000m",
    memory_limit="1Gi",
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

trigger_task = TriggerDagRunOperator(
    task_id="trigger_ably_reviews_dags",
    trigger_dag_id="fetch_and_save_ably_product_reviews_split",
    dag=dag,
)

goods_no_data_task = CustomKubernetesPodOperator(
    task_id=f"ably_goodsno_data_task",
    dag=dag,
    namespace="airflow",
    script_path="/python_scripts/ably/ably_goods_no_data.py",
    cpu_limit="1000m",
    memory_limit="1Gi",
    is_delete_operator_pod=True,
    get_logs=True,
)

# 디테일, 랭킹
ranking_and_detail_spark_submit_task = PythonOperator(
    task_id="ably_ranking_and_detail_data_spark_task",
    python_callable=submit_spark_application,
    dag=dag,
    op_args=[
        "ably-product-detail-raw-data-spark-submit-task",
        "ably/ably_ranking_and_product_detail_spark.py",
        None,
    ],
)

raking_goods_data_task >> trigger_task >> goods_no_data_task >> ranking_and_detail_spark_submit_task
