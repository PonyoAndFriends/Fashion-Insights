import os

# AWS S3 설정
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = "team3-2-s3"
REGION = "ap-northeast-2"

# S3 접속 정보를 딕셔너리로 정의
AWS_S3_CONFIG = {
    "aws_access_key_id": AWS_ACCESS_KEY,
    "aws_secret_access_key": AWS_SECRET_KEY,
    "region_name": REGION,
    "bucket_name": BUCKET_NAME,
}
