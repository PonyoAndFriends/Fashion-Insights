from airflow import DAG
from airflow.operators.python import PythonOperator
from ably.ably_modules.k8s_custom_python_pod_operator import CustomKubernetesPodOperator
from ably.ably_modules.k8s_spark_job_submit_operator import submit_spark_application
from datetime import datetime
import time
from ably.ably_modules.ably_dependencies import ABLYAPI_DEFAULT_ARGS


def sleep_time():
    time.sleep(60 * 1)
    return "kiki"


dag = DAG(
    dag_id="fetch_and_save_ably_product_reviews_split",
    default_args=ABLYAPI_DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)



review_detail_data_task = CustomKubernetesPodOperator(
    task_id=f"ably_reviews_detail_data_task",
    dag=dag,
    namespace="airflow",
    script_path="/python_scripts/ably/ably_review_detail_data.py",
    cpu_limit="1000m",
    memory_limit="1Gi",
    is_delete_operator_pod=True,
    get_logs=True,
)




review_spark_submit_task = PythonOperator(
    task_id="ably_product_review_data_spark_task",
    python_callable=submit_spark_application,
    dag=dag,
    op_args=[
        "ably-product-review-raw-data-spark-submit-task",
        "ably/ably_review_silverdata_spark.py",
        None,
    ],
)


review_detail_data_task >> review_spark_submit_task
