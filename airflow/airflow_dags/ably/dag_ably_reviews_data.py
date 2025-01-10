from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.dates import days_ago


def create_dag():
    # DAG 기본 설정
    dag = DAG(
        dag_id="fetch_and_save_product_reviews",
        default_args={
            "owner": "airflow",
            "start_date": days_ago(1),
            "retries": 2,
        },
        schedule_interval=None,
        catchup=False,
    )

    # 리뷰 데이터를 가져오는 태스크
    fetch_reviews_task = KubernetesPodOperator(
        task_id="fetch_reviews_task",
        name="fetch_reviews_task",
        namespace="default",
        image="python:3.9",
        cmds=["python", "-c"],
        arguments=[
            """
            import boto3
            import requests
            import json
            import time

            def fetch_reviews(product_id, api_base_url, headers, max_reviews=20, max_retries=3):
                url = f"{api_base_url}/{product_id}/reviews/"
                all_reviews = []

                for attempt in range(max_retries):
                    try:
                        response = requests.get(url, headers=headers)
                        if response.status_code != 200:
                            print(f"Error {response.status_code}: Retrying ({attempt + 1}/{max_retries})")
                            time.sleep(2)
                            continue

                        response_data = response.json()
                        reviews = response_data.get("reviews", [])
                        all_reviews.extend(reviews)

                        if len(all_reviews) >= max_reviews:
                            return all_reviews[:max_reviews]
                        break
                    except Exception as error:
                        print(f"Exception occurred: {error}")

                return all_reviews[:max_reviews]

            def save_reviews(product_id, reviews):
                file_name = f"reviews_{product_id}.json"
                json_content = json.dumps(reviews, ensure_ascii=False, indent=4)

                with open(file_name, "w", encoding="utf-8") as json_file:
                    json_file.write(json_content)

                print(f"Saved: {file_name}")

            def extract_product_ids_from_s3(bucket_name, s3_path):
                s3 = boto3.client('s3')
                response = s3.get_object(Bucket=bucket_name, Key=s3_path)
                data = json.loads(response['Body'].read().decode('utf-8'))
                return data.get("product_ids", [])

            # 설정값들
            api_base_url = "https://api.a-bly.com/webview/goods"
            request_headers = {
                "accept": "application/json, text/plain, */*",
                "x-anonymous-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhbm9ueW1vdXNfaWQiOiIzMzc3ODI3NDAiLCJpYXQiOjE3MzQ0MDE2MTR9.LZgtaLkf3KFnAcG4b5bcOa_taHF9FL6odYx5vd_o7w4",
                "user-agent": "Mozilla/5.0"
            }

            bucket_name = "ablyrawdata"
            s3_path = f"{time.strftime('%Y-%m-%d')}/Ably/ReviewData/data.json"

            product_id_list = extract_product_ids_from_s3(bucket_name, s3_path)
            for product_id in product_id_list:
                reviews = fetch_reviews(product_id, api_base_url, request_headers)
                if reviews:
                    save_reviews(product_id, reviews)
                else:
                    print(f"No reviews found for PRODUCT_ID={product_id}")
            """,
        ],
        is_delete_operator_pod=True,
        dag=dag,
    )

    return dag


dag = create_dag()
