from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from datetime import datetime
from ably_modules.ably_dependencies import ABLYAPI_DEFAULT_ARGS, DEFAULT_S3_DICT


def create_dag():
    dag = DAG(
        dag_id="fetch_and_save_ably_product_reviews_split",
        default_args=ABLYAPI_DEFAULT_ARGS,
        schedule_interval="@daily",
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )

    # S3 폴더 리스트 가져오기 작업
    list_folders_task = KubernetesPodOperator(
        task_id="list_folders_task",
        name="list_folders_task",
        namespace="default",
        image="coffeeisnan/python_pod_image:latest",
        cmds=["python", "-c"],
        arguments=[
            """
            import boto3
            import json
            from ably_modules.aws_info import AWS_S3_CONFIG

            def list_folders(bucket_name, base_path):
                s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=AWS_S3_CONFIG.get("aws_access_key_id"),
                    aws_secret_access_key=AWS_S3_CONFIG.get("aws_secret_access_key")
                )
                response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=base_path, Delimiter='/')
                return [content['Prefix'] for content in response.get('CommonPrefixes', [])]

            bucket_name = "{bucket_name}"
            base_path = "{base_path}"
            folders = list_folders(bucket_name, base_path)

            with open('/airflow/xcom/return.json', 'w') as f:
                json.dump({"folders": folders}, f)
            """.format(
                bucket_name=DEFAULT_S3_DICT["bucket_name"],
                base_path=f"{datetime.now().strftime('%Y-%m-%d')}/ReviewData/",
            ),
        ],
        is_delete_operator_pod=True,
        dag=dag,
    )

    # 상품 ID 추출 작업
    extract_product_ids_task = KubernetesPodOperator(
        task_id="extract_product_ids_task",
        name="extract_product_ids_task",
        namespace="default",
        image="coffeeisnan/python_pod_image:latest",
        cmds=["python", "-c"],
        arguments=[
            """
            import boto3
            import json
            from ably_modules.aws_info import AWS_S3_CONFIG

            def get_product_ids(bucket_name, folder):
                s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=AWS_S3_CONFIG.get("aws_access_key_id"),
                    aws_secret_access_key=AWS_S3_CONFIG.get("aws_secret_access_key")
                )
                key = f"{folder}goods_sno_list.json"
                response = s3_client.get_object(Bucket=bucket_name, Key=key)
                data = json.loads(response['Body'].read().decode('utf-8'))
                return next(iter(data.items()))

            bucket_name = "{bucket_name}"
            folders = {folders}

            product_ids_map = {}
            for folder in folders:
                category_key, product_ids = get_product_ids(bucket_name, folder)
                product_ids_map[folder] = {"category_key": category_key, "product_ids": product_ids}

            with open('/airflow/xcom/return.json', 'w') as f:
                json.dump(product_ids_map, f)
            """.format(
                bucket_name=DEFAULT_S3_DICT["bucket_name"],
                folders="{{ task_instance.xcom_pull(task_ids='list_folders_task')['folders'] }}",
            ),
        ],
        is_delete_operator_pod=True,
        dag=dag,
    )

    # 리뷰 데이터 처리 및 저장 작업
    process_reviews_task = KubernetesPodOperator(
        task_id="process_reviews_task",
        name="process_reviews_task",
        namespace="default",
        image="coffeeisnan/python_pod_image:latest",
        cmds=["python", "-c"],
        arguments=[
            """
            import requests
            import json
            import boto3
            import logging
            import time
            from ably_modules.ably_dependencies import ABLY_HEADER

            logging.basicConfig(level=logging.INFO)

            def fetch_reviews(api_url, headers, product_id, max_reviews=20, retries=3):
                reviews = []
                url = f"{api_url}/{product_id}/reviews/"
                for attempt in range(retries):
                    try:
                        response = requests.get(url, headers=headers)
                        if response.status_code == 200:
                            data = response.json().get("reviews", [])
                            reviews.extend(data)
                            if len(reviews) >= max_reviews:
                                return reviews[:max_reviews]
                            break
                    except Exception as e:
                        logging.error(f"Error fetching reviews for {product_id}: {e}")
                return reviews

            def save_reviews(s3_client, bucket_name, folder, category_key, product_id, reviews):
                key = f"{folder}reviews_{category_key}_{product_id}.json"
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=json.dumps(reviews, ensure_ascii=False, indent=4),
                    ContentType=\"application/json\"
                )
                logging.info(f"Saved reviews for {product_id} to {key}")

            api_url = "https://api.a-bly.com/webview/goods"
            headers = ABLY_HEADER
            product_ids_map = {product_ids_map}

            s3_client = boto3.client("s3")
            bucket_name = "{bucket_name}"

            for folder, data in product_ids_map.items():
                category_key = data["category_key"]
                product_ids = data["product_ids"]
                for product_id in product_ids:
                    reviews = fetch_reviews(api_url, headers, product_id)
                    if reviews:\n
                        save_reviews(s3_client, bucket_name, folder, category_key, product_id, reviews)
            """.format(
                product_ids_map="{{ task_instance.xcom_pull(task_ids='extract_product_ids_task') }}",
                bucket_name=DEFAULT_S3_DICT["bucket_name"],
            ),
        ],
        is_delete_operator_pod=True,
        dag=dag,
    )

    # 작업 간의 의존성 설정
    list_folders_task >> extract_product_ids_task >> process_reviews_task

    return dag


dag = create_dag()
