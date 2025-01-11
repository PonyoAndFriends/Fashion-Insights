from airflow import DAG
from ably_modules.k8s_custom_python_pod_operator import CustomKubernetesPodOperator
from datetime import datetime
from ably.ably_modules.ably_dependencies import ABLYAPI_DEFAULT_ARGS

dag = DAG(
    dag_id="fetch_and_save_ably_product_reviews_split",
    default_args=ABLYAPI_DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

raking_goods_data_task = CustomKubernetesPodOperator(
    task_id=f"alby_reviews_data_task",
    namespace="airflow",
    script_path="/python_scripts/ably/ably_review_data.py",
    cpu_limit="1000m",
    memory_limit="1Gi",
    is_delete_operator_pod=True,
    get_logs=True,
)

raking_goods_data_task
