from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.operators.dummy import DummyOperator
from musinsa.custom_operators.k8s_spark_job_submit_operator import (
    SparkApplicationOperator,
)
from musinsa.custom_operators.k8s_custom_python_pod_operator import (
    CustomKubernetesPodOperator,
)
import json
from datetime import datetime

from musinsa.modules.musinsa_mappingtable import (
    CATEGORY2DEPTH_MAPPING,
    mapping2depth_en,
    mapping3depth_en,
)
from musinsa.modules.config import DEFAULT_DAG

# DAG 정의
with DAG(
    dag_id="Musinsa_ProductReview_RawData_EL_DAG",
    default_args=DEFAULT_DAG.DEFAULT_ARGS,
    description="musinsa ranking raw data extraction and loading to s3",
    schedule_interval="0 0 * * *",  # 수정 필요
    start_date=datetime(2025, 1, 1, tzinfo=DEFAULT_DAG.LOCAL_TZ),
    catchup=False,
    tags=["MUSINSA", "REVIEW_RAWDATA", "EXTRACT", "LOAD", "S3", "K8S"],
) as dag:

    # start task
    start = DummyOperator(task_id="start")

    # raw_end task
    raw_end = DummyOperator(task_id="raw_end")

    # dag_end task
    dag_end = DummyOperator(task_id="dag_end")

    for key in CATEGORY2DEPTH_MAPPING:
        category2depth = key
        category3depth_list = list(CATEGORY2DEPTH_MAPPING[key].items())

        category2depth_task = DummyOperator(
            task_id=f"{mapping2depth_en(category2depth)}_task"
        )

        wait_task = DummyOperator(task_id=f"{mapping2depth_en(category2depth)}_wait")

        start >> category2depth_task

        for category3depth in category3depth_list:
            category3depth_task = CustomKubernetesPodOperator(
                task_id=f"review_{mapping3depth_en(category3depth[0])}_task",
                namespace="airflow",
                script_path="/python_scripts/musinsa/musinsa_productreview_rawdata_el.py",
                required_args={
                    "category_3_depth": category3depth[0],
                    "category_4_depth_list": json.dumps(category3depth[1]),
                },
                cpu_limit="1000m",
                memory_limit="1Gi",
                is_delete_operator_pod=True,
                get_logs=True,
            )

            category2depth_task >> category3depth_task >> wait_task

        wait_task >> raw_end

    spark_submit_task = SparkApplicationOperator(
        name="musinsa_product_review_raw_data_spark_submit_task",
        main_application_file="musinsa/musinsa_produdctreview_silverdata_spark.py",
        task_id="musinsa_product_review_data_spark_task",
    )

    raw_end >> spark_submit_task >> dag_end
