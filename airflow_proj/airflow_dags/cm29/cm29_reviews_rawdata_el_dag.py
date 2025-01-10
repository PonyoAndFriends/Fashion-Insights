import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from cm29.cm29_reviews_rawdata_el import (
    list_files_in_s3,
    fetch_and_save_reviews_from_all_files,
)

REVIEW_FOLDER_PATH = "29cm_reviews"
PLATFORM_FOLDER_PATH = "29cm"

# DAG 기본 설정
default_args = {
    "owner": "pcy7805@naver.com",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="29cm_Reviews_Raw_Data_EL_DAG",
    default_args=default_args,
    description="29cm reviews data extraction and loading to s3",
    schedule_interval="0 0 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["29CM", "REVIEWS_RAWDATA", "EXTRACT", "LOAD", "S3"],
) as dag:

    today = datetime.now().strftime("%Y-%m-%d")

    genders = ["Woman", "Man"]
    for gender in genders:
        for medium_folder in ["Pants", "Tops", "Shoes", "Outerwear", "Knitwear"]:
            folder_path = f"bronze_layer/{today}/{PLATFORM_FOLDER_PATH}/{REVIEW_FOLDER_PATH}/{gender}/{medium_folder}"
            json_files = list_files_in_s3(folder_path)

            with TaskGroup(group_id=f"{gender}_{medium_folder}_group") as medium_group:
                for file_key in json_files:
                    if file_key.endswith("_ids.json"):
                        category_name = os.path.basename(file_key).replace(
                            "_ids.json", ""
                        )
                        PythonOperator(
                            task_id=f"fetch_reviews_{gender}_{medium_folder}_{category_name}",
                            python_callable=fetch_and_save_reviews_from_all_files,
                            op_args=[file_key],
                        )
