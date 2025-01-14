from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from redshift_dags.custom_sql_operators.custom_query_operator import (
    RedshiftQueryOperator,
)
from redshift_dags.custom_sql_modules.query_dag_dependencies import (
    SILVER_LOAD_DEFAULT_ARGS,
    DEFAULT_SILVER_SHCEMA,
    NOW_STRING,
    DEFULAT_SILVER_BUCKET_URL,
)

mapping_list = [
    "기타",
    "니트",
    "셔츠",
    "스커트",
    "원피스",
    "재킷",
    "전체",
    "코트",
    "티셔츠",
    "패딩",
    "팬츠",
    "폴리스",
]

default_args = SILVER_LOAD_DEFAULT_ARGS

with DAG(
    dag_id="musinsa_ranking_tb_silver_data_to_redshift_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    concurrency=1,
) as dag:

    # 기본적인 설정 정의
    now_string = NOW_STRING
    silver_bucket_url = DEFULAT_SILVER_BUCKET_URL
    platforms = ["29cm", "musinsa"]
    redshift_iam_role = Variable.get("redshift_iam_role")
    platform = "musinsa"
    table = "ranking_tb"

    start = DummyOperator(task_id="start")

    for task_number, depth3code in enumerate(mapping_list):
        copy_query = f"""
        COPY {DEFAULT_SILVER_SHCEMA}.{table}
        FROM '{silver_bucket_url}/{now_string}/{platform}/ranking_tb/{depth3code}/'
        IAM_ROLE '{redshift_iam_role}'
        FORMAT AS PARQUET;
        """

        copy_task = RedshiftQueryOperator(
            task_id=f"{platform}_{task_number}_ranking_tb_copy_task",
            sql=copy_query,
        )

        start >> copy_task
