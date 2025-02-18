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
    dag_id="total_product_gold_table_dag",
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
        r.platform, -- 수집 플랫폼
        SPLIT_PART(p.master_category_name, '-', 1) AS cat_depth_1,
        SPLIT_PART(p.master_category_name, '-', 2) AS cat_depth_2,
        SPLIT_PART(p.master_category_name, '-', 3) AS cat_depth_3,
        p.small_category_name, -- 4차 카테고리
        r.product_id, -- 상품 ID
        p.product_name, -- 상품 이름
        r.ranking, -- 랭킹
        p.img_url, -- 이미지 URL
        p.brand_name_kr, -- 브랜드 이름 (한글)
        p.original_price, -- 소비자 가격
        p.final_price, -- 최종 소비자 가격
        p.discount_ratio, -- 상품 할인율
        p.review_counting, -- 리뷰 개수
        p.review_avg_rating, -- 리뷰 평점
        p.like_counting, -- 좋아요 개수
        r.created_at -- 수집 날짜
    FROM
        {DEFAULT_SILVER_SHCEMA}.ranking_tb r
    LEFT JOIN
        {DEFAULT_SILVER_SHCEMA}.master_category_tb mc
    ON
        r.master_category_name = mc.master_category_name
    LEFT JOIN
        {DEFAULT_SILVER_SHCEMA}.product_detail_tb p
    ON
        r.product_id = p.product_id
    AND r.platform = p.platform
    AND r.master_category_name = p.master_category_name;
    """

    full_refresh_task = RefreshTableOperator(
        task_id="total_product_gold_table_task",
        drop_sql=drop_sql,
        create_sql=create_sql,
        redshift_conn_id="redshift_default",
    )

    full_refresh_task
