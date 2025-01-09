from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from custom_operators.k8s_spark_job_submit_operator import SparkApplicationOperator
from custom_operators.k8s_custom_python_pod_operator import CustomKubernetesPodOperator

from datetime import datetime

# 라이브러리 path 수정
from modules.musinsa_mappingtable import SEXUAL_CATEGORY_DYNAMIC_PARAMS
from modules.config import DEFAULT_DAG

import json

# DAG 정의
with DAG(
    dag_id="Musinsa_Ranking_RawData_EL_DAG",
    default_args=DEFAULT_DAG.default_args,  # path 수정에 따라 좀 필수
    description="musinsa ranking raw data extraction and loading to s3",
    schedule_interval="0 0 * * *",  # 수정 필요
    start_date=datetime(2025, 1, 1, tzinfo=DEFAULT_DAG.local_tz),
    catchup=False,
    tags=["MUSINSA", "RANKING_RAWDATA", "EXTRACT", "LOAD", "S3", "K8S"],
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
                script_path="./python_scripts/musinsa/musinsa_ranking_rawdata_el.py",
                arguments={
                    "sexual": json.dumps(sexual), 
                    "category_data": json.dumps(category2depth)
                    },
                cpu_limit="1000m",
                memory_limit="1Gi",
                cpu_request="500m",
                memory_request="512Mi",
                is_delete_operator_pod=True,
                get_logs=True,
            )

            sexual_task >> category_task >> wait_task

        wait_task >> raw_end

    spark_submit_task = SparkApplicationOperator(
        name="musinsa_ranking_data_spark_application",
        main_application_file="musinsa/musinsa_ranking_silverdata_spark.py",
        task_id="musinsa_ranking_data_spark_task",
    )

    trigger_tasks = [
        "Musinsa_ProductReview_RawData_EL_DAG",
        "Musinsa_ProductDetail_RawData_EL_DAG",
    ]
    for dag_id in []:
        trriger_task = TriggerDagRunOperator(
            trigger_dag_id=dag_id,
        )

    raw_end >> spark_submit_task >> trigger_tasks >> dag_end