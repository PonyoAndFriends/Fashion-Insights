from pyspark.sql import SparkSession
import boto3
import json

# SparkSession 생성
spark = SparkSession.builder \
    .appName("S3 Write Dict") \
    .getOrCreate()

# 딕셔너리 생성
data_dict = {
    'name': 'Airflow-Spark',
    'task': 'write_to_s3',
    'status': 'success'
}

# S3 클라이언트 설정
s3_client = boto3.client('s3')

# S3 버킷과 객체 경로 정의
bucket_name = 'spark-test-hs'
file_key = 'testing_folder/dict_data.json'

# 딕셔너리를 JSON 형식으로 S3에 저장
s3_client.put_object(
    Bucket=bucket_name,
    Key=file_key,
    Body=json.dumps(data_dict),
    ContentType='application/json'
)

print(f"Data written to S3 bucket {bucket_name} at {file_key}")
