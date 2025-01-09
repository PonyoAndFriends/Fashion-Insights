from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from cm29_ranking_rawdata_el_dag import fetch_and_save_data_to_s3
from cm29_mapping_table import CATEGORY_TREE

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
    dag_id="29cm_Ranking_Raw_Data_EL_DAG",
    default_args=default_args,
    description="29cm ranking raw data extraction and loading to s3",
    schedule_interval="0 0 * * *",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    for large_category, category_info in CATEGORY_TREE.items():
        large_id = category_info["large_id"]
        gender_folder = "Woman" if "Woman" in large_category else "Man"
        # TaskGroup 생성
        with TaskGroup(group_id=f"{large_category}_group") as task_group:
            for medium_category, medium_info in category_info["subcategories"].items():
                # Clothes 카테고리인지 확인하여 처리
                if isinstance(medium_info, dict) and "subcategories" in medium_info:
                    medium_id = medium_info["large_id"]

                    for small_category, small_id in medium_info[
                        "subcategories"
                    ].items():
                        s3_path = f"{medium_category}/{small_category}"
                        task_id = f"{large_category}_{medium_category}_{small_category}".replace(
                            "/", "_"
                        )
                        PythonOperator(
                            task_id=task_id,
                            python_callable=fetch_and_save_data_to_s3,
                            op_args=[
                                large_id,
                                medium_id,
                                small_id,
                                s3_path,
                                gender_folder,
                            ],
                        )
                # Shoes 카테고리인지 확인하여 처리
                elif isinstance(medium_info, str):
                    s3_path = medium_category
                    task_id = f"{large_category}_{medium_category}".replace("/", "_")
                    PythonOperator(
                        task_id=task_id,
                        python_callable=fetch_and_save_data_to_s3,
                        op_args=[large_id, medium_info, None, s3_path, gender_folder],
                    )
