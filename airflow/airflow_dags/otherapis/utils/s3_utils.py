def load_data_to_s3(aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket_name, data_file, file_path, content_type):
    """
    AWS S3에 데이터를 업로드하는 함수

    Args:
        aws_access_key_id (str): AWS Access Key ID
        aws_secret_access_key (str): AWS Secret Access Key
        aws_region (str): AWS Region
        s3_bucket_name (str): S3 버킷 이름
        data_file (str): 업로드할 데이터
        file_path (str): S3 내 저장 경로
        content_type (str): 데이터의 Content-Type

    Returns:
        None
    """
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError, EndpointConnectionError
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # S3 클라이언트 초기화
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )

    try:
        # 데이터 업로드
        s3.put_object(
            Bucket=s3_bucket_name,
            Key=file_path,
            Body=data_file,
            ContentType=content_type
        )
        logging.info(f"Successfully uploaded to S3: {s3_bucket_name}/{file_path}")

    except ClientError as e:
        logging.error(f"ClientError: Failed to upload to S3. {e.response['Error']['Message']}")
        raise
    except EndpointConnectionError as e:
        logging.error(f"EndpointConnectionError: Could not connect to the endpoint. {str(e)}")
        raise
    except BotoCoreError as e:
        logging.error(f"BotoCoreError: A general error occurred. {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        raise
