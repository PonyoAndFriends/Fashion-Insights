from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from musinsa.custom_operators.k8s_spark_job_submit_operator import (
    submit_spark_application,
)
from musinsa.custom_operators.k8s_custom_python_pod_operator import (
    CustomKubernetesPodOperator,
)

from datetime import datetime

# 라이브러리 path 수정
from musinsa.modules.musinsa_mappingtable import (
    SEXUAL_CATEGORY_DYNAMIC_PARAMS,
)
from musinsa.modules.config import DEFAULT_DAG

import json
import time


def sleep_20_min():
    time.sleep(20 * 60)
    return "동희님 바보"


# DAG 정의
with DAG(
    dag_id="Musinsa_Ranking_RawData_EL_DAG",
    default_args=DEFAULT_DAG.DEFAULT_ARGS,  # path 수정에 따라 좀 필수
    description="musinsa ranking raw data extraction and loading to s3",
    schedule_interval="0 0 * * *",  # 수정 필요
    start_date=datetime(2025, 1, 1, tzinfo=DEFAULT_DAG.LOCAL_TZ),
    catchup=False,
    tags=["MUSINSA", "RANKING_RAWDATA", "EXTRACT", "LOAD", "S3", "K8S"],
    concurrency=5,
    max_active_runs=1,
) as dag:

    # start task
    start = DummyOperator(task_id="start")

    # raw_end task
    raw_end = DummyOperator(task_id="raw_end")

    # raw_end task
    dag_end = DummyOperator(task_id="silver_end")

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
                task_id=f"{sexual[0]}_{category2depth[0]}_task",
                namespace="airflow",
                script_path="/python_scripts/musinsa/musinsa_ranking_rawdata_el.py",
                required_args={
                    "sexual": json.dumps(sexual),
                    "category_data": json.dumps(category2depth),
                },
                cpu_limit="1000m",
                memory_limit="1Gi",
                is_delete_operator_pod=True,
                get_logs=True,
            )

            sexual_task >> category_task >> wait_task

        wait_task >> raw_end

    spark_application_task = PythonOperator(
        task_id="musinsa_raking_rawdata_el_spark",
        python_callable=submit_spark_application,
        op_args=[
            "musinsa-ranking-rawdata-el-spark",
            "musinsa/musinsa_ranking_silverdata_spark.py",
            None,
        ],
    )

    trigger_dag_ids = [
        "Musinsa_ProductReview_RawData_EL_DAG",
        "Musinsa_ProductDetail_RawData_EL_DAG",
    ]

    trigger_tasks = []

    sleep_task = PythonOperator(
        task_id="wait_for_trigger", python_callable=sleep_20_min
    )

    for i, dag_id in enumerate(trigger_dag_ids):
        trigger_task = TriggerDagRunOperator(
            task_id=f"triggers_other_dags_{i}",
            trigger_dag_id=dag_id,
        )
        trigger_tasks.append(trigger_task)

    raw_end >> spark_application_task >> sleep_task >> trigger_tasks >> dag_end
