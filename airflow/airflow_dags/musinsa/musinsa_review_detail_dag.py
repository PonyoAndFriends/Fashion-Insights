from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from datetime import datetime
import math

# 병렬 실행할 태스크의 개수
PARALLEL_TASK_NUM = 5

# 적당히 알맞게 수정 필요
default_args = {
    "owner": "dongheekim@mola.com",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}


def split_product_ids(**context):
    """
    product id의 리스트를 적절하게 나누는 함수 => 파이썬 오퍼레이터로 airflow worker 내에서 실행 (간단한 로직)
    """
    # 이전 대그에서 전달받는 성별, 카테고리, 프로덕트 id 리스트(약 50개를 가정)
    gender = context["dag_run"].conf.get("gender", "unknown")  # 'M', 'F', or 'A'
    category = context["dag_run"].conf.get("category", "unknown")  # e.g., '103000'
    product_ids = context["dag_run"].conf.get(
        "product_ids", []
    )  # List of 50 product IDs

    # 5개의
    chunk_size = math.ceil(len(product_ids) / PARALLEL_TASK_NUM)  #
    product_id_chunks = [
        product_ids[i : i + chunk_size] for i in range(0, len(product_ids), chunk_size)
    ]

    # Push data to XCom for downstream tasks
    context["ti"].xcom_push(key="gender", value=gender)
    context["ti"].xcom_push(key="category", value=category)
    context["ti"].xcom_push(key="product_id_chunks", value=product_id_chunks)


# Define the DAG
with DAG(
    dag_id="dynamic_k8s_pod_parallel_processing",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    # Step 1: Split product IDs
    split_ids_task = PythonOperator(
        task_id="split_product_ids",
        python_callable=split_product_ids,
        provide_context=True,
    )

    def create_pod_task(index, chunk):
        return KubernetesPodOperator(
            task_id=f"fetch_review_datail_task_{index}",
            namespace="airflow",
            image="coffeeisnan/project4-custom:latest",
            cmds=["python", "./pythonscript/musinsa_review_detail.py"],
            arguments=[
                "--gender={{ ti.xcom_pull(task_ids='split_product_ids', key='gender') }}",
                "--category={{ ti.xcom_pull(task_ids='split_product_ids', key='category') }}",
                f"--product_ids={chunk}",
            ],
            name=f"fetch_review_datail_pod_{index}",
            is_delete_operator_pod=True,
            in_cluster=True,
        )

    # Dynamically create tasks for each chunk
    for i in range(1, PARALLEL_TASK_NUM + 1):
        pod_task = create_pod_task(
            index=i,
            chunk="{{ ti.xcom_pull(task_ids='split_product_ids', key='product_id_chunks')["
            + str(i)
            + "] }}",
        )
        split_ids_task >> pod_task
