def load_data_to_s3(aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket_name, data_file, file_path, content_type):
    from botocore.exceptions import ClientError, BotoCoreError, EndpointConnectionError
    import boto3

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    
    try:
        s3.put_object(
            Bucket=s3_bucket_name,
            Key=file_path,
            Body=data_file,
            ContentType=content_type
        )
        print(f"{data_file} is successfully uploaded to s3 bueckt {s3_bucket_name}")
    except ClientError as e:
        print(f"ClientError: Failed to upload to S3. {e.response['Error']['Message']}")
    except EndpointConnectionError as e:
        print(f"EndpointConnectionError: Could not connect to the endpoint. {str(e)}")
    except BotoCoreError as e:
        print(f"BotoCoreError: A general error occurred. {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
