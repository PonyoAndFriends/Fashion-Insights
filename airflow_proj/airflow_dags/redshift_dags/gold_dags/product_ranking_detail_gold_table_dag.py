from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from redshift_dags.custom_sql_operators.custom_refresh_table_operator import (
    RefreshTableOperator,
)
from redshift_dags.custom_sql_modules.query_dag_dependencies import (
    SILVER_LOAD_DEFAULT_ARGS,
    DEFAULT_SILVER_SHCEMA,
    DEFAULT_GOLD_SHCEMA,
)

# DAG 기본 설정
default_args = SILVER_LOAD_DEFAULT_ARGS

# DAG 정의
with DAG(
    dag_id="product_ranking_detail_gold_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # 기본적인 설정 정의
    table = "total_product_gold_tb"
    redshift_iam_role = Variable.get("redshift_iam_role")

    drop_sql = f"""
    DROP TABLE IF EXISTS {DEFAULT_GOLD_SHCEMA}.{table};
    """

    create_sql = f"""
    CREATE TABLE {DEFAULT_GOLD_SHCEMA}.{table} AS
    SELECT
        r.platform,
        SPLIT_PART(p.master_category_name, '-', 1) AS cat_depth_1,
        SPLIT_PART(p.master_category_name, '-', 2) AS cat_depth_2,
        SPLIT_PART(p.master_category_name, '-', 3) AS cat_depth_3,
        p.small_category_name,
        r.product_id,
        p.product_name,
        r.ranking,
        p.img_url,
        p.brand_name_kr,
        p.original_price,
        p.final_price,
        p.discount_ratio,
        p.review_counting,
        p.review_avg_rating,
        p.like_counting,
        r.created_at
    FROM 
        {DEFAULT_SILVER_SHCEMA}.ranking_tb r
    JOIN 
        retail_silver_layer.product_detail_tb p
    ON 
        r.product_id = p.product_id
        AND r.platform = p.platform;
    """

    full_refresh_task = RefreshTableOperator(
        task_id="prodeuct_ranking_detail_gold_task",
        drop_sql=drop_sql,
        create_sql=create_sql,
        redshift_conn_id="redshift_default",
    )

    full_refresh_task
