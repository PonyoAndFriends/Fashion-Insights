from airflow.models import Variable

# AWS S3 설정
AWS_ACCESS_KEY = Variable.get("aws_access_key_id")
AWS_SECRET_KEY = Variable.get("aws_secret_access_key")
BUCKET_NAME = Variable.get("s3_bucket")
REGION = Variable.get("aws_region")

# S3 접속 정보를 딕셔너리로 정의
AWS_S3_CONFIG = {
    "aws_access_key_id": AWS_ACCESS_KEY,
    "aws_secret_access_key": AWS_SECRET_KEY,
    "region_name": REGION,
    "bucket_name": BUCKET_NAME,
}
