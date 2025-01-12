from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.dummy import DummyOperator

from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

from musinsa.modules.musinsa_mappingtable import CATEGORY2DEPTH_MAPPING, mapping2depth_en, mapping3depth_en

# DAG 기본 설정
default_args = {
    'owner': 'ehdgml7755@cu.ac.kr',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 24,
    'retry_delay': timedelta(minutes=30),
}

# today_date
today_date = (datetime.now()).strftime("%Y-%m-%d")

with DAG(
    dag_id='Musinsa_ProductDetail_Table_S3_Load_Redshift',
    default_args=default_args,
    description='musinsa ranking raw data extraction and loading to s3',
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['MUSINSA', 'SILVERDATA', 'PRODUCTDETAIL', 'LOAD', 'S3', 'REDSHIFT']
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    end = DummyOperator(
        task_id="end"
    )
    
    for category2depth in CATEGORY2DEPTH_MAPPING:
        categorydepth_task = DummyOperator(
            task_id=f'{mapping2depth_en(category2depth)}_task'
        )
        
        wait = DummyOperator(
            task_id=f"{mapping2depth_en(category2depth)}_wait"
        )
        
        start >> categorydepth_task
        for category3depth in CATEGORY2DEPTH_MAPPING[category2depth]:
            
            s3_copy_redshift_task = S3ToRedshiftOperator(
                task_id=f"load_ranking_{mapping3depth_en(category3depth)}_data",
                schema="retail_silver_layer",
                table="product_detail_tb",
                s3_bucket="project4-silver-data",
                s3_key=f"{today_date}/Musinsa/ProductDetailData/{category3depth}/",
                copy_options=['FORMAT AS PARQUET'],
                aws_conn_id="aws_default",
                redshift_conn_id="redshift_default",
                task_concurrency=1 # 동시 태스크 실행 개수 => 단순 copy지만 쿼리 부하가 되려나...? 우선 걸어두었습니다.
            )
            
        categorydepth_task >> s3_copy_redshift_task >> wait
        
    wait >> end
