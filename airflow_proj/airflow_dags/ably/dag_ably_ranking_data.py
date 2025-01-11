from airflow import DAG
from ably.ably_modules.k8s_custom_python_pod_operator import CustomKubernetesPodOperator
from datetime import datetime
from itertools import islice
from ably_modules.ably_dependencies import (
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
)

raking_goods_data_task
