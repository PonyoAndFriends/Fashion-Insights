from botocore.exceptions import ClientError, BotoCoreError, EndpointConnectionError
from airflow.models import Variable

import boto3
import logging

logger = logging.getLogger(__name__)

def load_data_to_s3(s3_dict):
    """
    boto3를 사용하여 s3에 데이터를 적재하는 함수
    
    :param s3_dict: s3에 데이터를 적재하기 위한 설정을 담은 딕셔너리
        data_file, file_path, content_type를 키로 받아옴.
    """
    logger.info("Starting S3 data upload process.")

    try:
        # S3 관련 내용은 Variable로 관리
        aws_access_key_id = Variable.get("aws_access_key_id")
        aws_secret_access_key = Variable.get("aws_secret_access_key")
        aws_region = Variable.get("aws_region")
        s3_bucket_name = Variable.get("s3_bucket_name")

        logger.debug("AWS S3 bucket configurations successfully loaded.")

        # s3_dict 이라는 딕셔너리로 필요한 인자를 받음
        data_file, file_path, content_type = [s3_dict.get(key, None) for key in s3_dict]
        logger.debug(f"Data file: {data_file}, File path: {file_path}, Content type: {content_type}")

        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        logger.debug("S3 client initialized.")

        s3.put_object(
            Bucket=s3_bucket_name,
            Key=file_path,
            Body=data_file,
            ContentType=content_type
        )
        logger.info(f"{data_file} uploaded to s3 bucket {s3_bucket_name} at file path: {file_path}.")

    # 예외처리 및 로깅
    except ClientError as e:
        error_message = e.response['Error']['Message']
        logger.error(f"ClientError: Failed to upload to S3. {error_message}")
        raise
    except EndpointConnectionError as e:
        logger.error(f"EndpointConnectionError: Could not connect to the endpoint. {str(e)}")
        raise
    except BotoCoreError as e:
        logger.error(f"BotoCoreError: A general error occurred. {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise
