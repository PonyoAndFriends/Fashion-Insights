from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from musinsa.custom_operators.k8s_spark_job_submit_operator import (
    submit_spark_application
)
from musinsa.custom_operators.k8s_custom_python_pod_operator import (
    CustomKubernetesPodOperator,
)

from musinsa.modules.musinsa_mappingtable import (
    SEXUAL_CATEGORY_DYNAMIC_PARAMS,
)
from musinsa.modules.config import DEFAULT_DAG

import json
from datetime import datetime

with DAG(
    dag_id="Musinsa_ProductDetail_RawData_EL_DAG",
    default_args=DEFAULT_DAG.DEFAULT_ARGS,
    description="musinsa ranking raw data extraction and loading to s3",
    schedule_interval="0 0 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=DEFAULT_DAG.LOCAL_TZ),
    catchup=False,
    tags=[
        "MUSINSA",
        "PRODUCTDETAIL_RAWDATA",
        "EXTRACT",
        "TRANSFORM",
        "LOAD",
        "S3",
        "K8S",
    ],
) as dag:

    # start task
    start = DummyOperator(task_id="start")

    # raw_end task
    raw_end = DummyOperator(task_id="raw_end")

    # dag_end task
    dag_end = DummyOperator(task_id="dag_end")

    # SEXUAL_CATEGORY_DYNAMIC_PARAMS
    # dct_1 => 여성 dct / dct_2 => 남성 dct
    for dct in SEXUAL_CATEGORY_DYNAMIC_PARAMS:
        sexual = list(dct["SEXUAL"].items())[0]

        sexual_task = DummyOperator(task_id=f"{sexual[0]}_task")

        wait_task = DummyOperator(task_id=f"{sexual[0]}_wait")

        start >> sexual_task
        for categories in dct["CATEGORIES"]:
            category2depth = list(categories.items())[0]

            category_task = CustomKubernetesPodOperator(
                task_id=f"product_detail_{sexual[0]}_{category2depth[0]}_task",
                namespace="airflow",
                script_path="/python_scripts/musinsa/musinsa_productdetail_rawdata_etl.py",
                required_args={
                    "sexual_dict": json.dumps(sexual),
                    "category_2_depth": json.dumps(category2depth),
                },
                cpu_limit="1000m",
                memory_limit="1Gi",
                is_delete_operator_pod=True,
                get_logs=True,
            )

            sexual_task >> category_task >> wait_task

        wait_task >> raw_end

    spark_submit_task = PythonOperator(
        task_id="musinsa_product_detail_data_spark_task",
        python_callable=submit_spark_application,
        op_args=[
            "musinsa-product-detail-raw-data-spark-submit-application",
            "musinsa/musinsa_productdetail_silverdata_spark.py",
            None,
        ],
    )

    raw_end >> spark_submit_task >> dag_end
