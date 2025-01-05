from botocore.exceptions import ClientError, BotoCoreError, EndpointConnectionError
import boto3
import logging

logger = logging.getLogger(__name__)


def load_data_to_s3(s3_dict):
    """
    boto3를 사용하여 s3에 데이터를 적재하는 함수

    :param s3_dict: s3에 데이터를 적재하기 위한 설정을 담은 딕셔너리
        aws_access_key_id: s3 접근 key id
        aws_secret_access_key: s3 접근 secret access key
        aws_region: aws의 s3 region
        s3_bucket_name: 데이터를 적재할 S3 버킷 이름
        data_file: 데이터 적재 내용
        file_path: S3 내의 데이터 적재 경로
        content_type: api가 반환하는 데이터의 타입
    """
    logger.info("Starting S3 data upload process.")

    try:
        # s3_dict 로 아래와 같은 인자를 받음.
        (
            aws_access_key_id,
            aws_secret_access_key,
            aws_region,
            s3_bucket_name,
            data_file,
            file_path,
            content_type,
        ) = [s3_dict.get(key, None) for key in s3_dict]
        logger.debug("AWS S3 bucket configurations successfully loaded.")
        logger.debug(f"s3_dict : {s3_dict}")

        # data_file, file_path, content_type 로깅
        logger.debug(
            f"Data file: {data_file}, File path: {file_path}, Content type: {content_type}"
        )

        # boto를 활용하여 S3에 파일 적재
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
        )
        logger.debug("S3 client initialized.")

        s3.put_object(
            Bucket=s3_bucket_name,
            Key=file_path,
            Body=data_file,
            ContentType=content_type,
        )
        logger.info(
            f"{data_file} uploaded to s3 bucket {s3_bucket_name} at file path: {file_path}."
        )

    # 에러 로깅
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(f"ClientError: Failed to upload to S3. {error_message}")
        raise
    except EndpointConnectionError as e:
        logger.error(
            f"EndpointConnectionError: Could not connect to the endpoint. {str(e)}"
        )
        raise
    except BotoCoreError as e:
        logger.error(f"BotoCoreError: A general error occurred. {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

